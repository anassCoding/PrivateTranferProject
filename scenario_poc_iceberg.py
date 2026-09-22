"""
Dataiku DSS SCENARIO STEP ("Execute Python code" step): for each "flow" listed
in an input CSV-backed dataset, look inside a managed folder (backed by HDFS)
at /<flow>/<timestamp_subfolder>/ and find the largest subfolder timestamp
that is still greater than the flow's reference timestamp stored in the
project variables.
 
Behaviour required for the scenario:
  - If no flow has a subfolder newer than its recorded variable -> the
    scenario is stopped here (gracefully aborted), and none of the
    following scenario steps run.
  - If at least one flow has a newer subfolder -> the corresponding project
    variable(s) are updated to the new max timestamp, and the scenario
    continues to its next step.
 
Drop this whole script into a scenario's "Execute Python code" step.
Adjust the CONFIG block below to match your project.
"""
 
import dataiku
from dataiku.scenario import Scenario
import pandas as pd
from datetime import datetime
 
# ============================= CONFIG ======================================
 
# Managed folder id (or name) that points at the HDFS directory containing
# one subfolder per flow. Find the id in the folder's Settings page, or just
# use dataiku.Folder("folder_name_as_shown_in_flow") if names are unique.
FOLDER_ID = "YOUR_FOLDER_ID"
 
# Name of the input dataset built on top of your CSV (the list of flows).
FLOWS_DATASET_NAME = "flows_to_process"
 
# Column in that dataset holding the flow name (must match the subfolder
# name under the managed folder root, e.g. "/my_flow/...").
FLOW_COLUMN_NAME = "flow_name"
 
# How subfolder names encode a timestamp. Two supported modes:
#   1) A strptime-compatible string format, e.g. "%Y%m%d_%H%M%S"
#   2) None -> treat the folder name as an epoch integer (seconds or millis)
TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
 
# How the reference timestamp is named in the project variables.
# If your variables are keyed simply as "<flow>", set this to "{flow}".
# If keyed like "my_flow_last_ts", use "{flow}_last_ts", etc.
VARIABLE_KEY_TEMPLATE = "{flow}_timestamp"
 
# Optional: name of an output dataset to write results to. Set to None to
# just print results instead of writing an output dataset.
OUTPUT_DATASET_NAME = None  # e.g. "flow_max_timestamps"
 
# What "proceed" means when several flows are checked in the same run:
#   "all" -> only proceed (and update variables) if EVERY flow has a
#            subfolder newer than its recorded timestamp. If even one flow
#            has nothing new, the whole scenario stops here.
#   "any" -> proceed if AT LEAST ONE flow has a newer subfolder. Only the
#            qualifying flows' variables get updated; flows with nothing
#            new are simply left untouched (not blocking).
PROCEED_MODE = "all"
 
# =============================================================================
 
 
def parse_timestamp(value, fmt=TIMESTAMP_FORMAT):
    """Parse a folder-name (or variable value) into a datetime for comparison.
    Returns None if it can't be parsed."""
    value = str(value).strip()
    if fmt:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            return None
    else:
        try:
            ts = int(value)
            if ts > 10 ** 12:  # looks like milliseconds
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts)
        except ValueError:
            return None
 
 
def get_project_and_variables():
    client = dataiku.api_client()
    project = client.get_project(dataiku.default_project_key())
    variables = project.get_variables()
    return project, variables
 
 
def list_subfolders(folder, flow_path):
    """Return list of (name, full_path) for immediate subfolders of flow_path."""
    details = folder.get_path_details(path=flow_path)
    if not details.get("exists") or not details.get("directory"):
        return None  # flow folder missing or not a directory
 
    subfolders = []
    for child in details.get("children", []):
        if not child.get("directory"):
            continue  # skip plain files, only interested in timestamp subfolders
        name = child["path"].rstrip("/").split("/")[-1]
        subfolders.append((name, child["path"]))
    return subfolders
 
 
def main():
    # Handle to the scenario that is currently running this step.
    scenario = Scenario()
 
    project, variables = get_project_and_variables()
    project_vars = variables.get("standard", {})
 
    flows_ds = dataiku.Dataset(FLOWS_DATASET_NAME)
    flows_df = flows_ds.get_dataframe()
    flow_names = flows_df[FLOW_COLUMN_NAME].dropna().astype(str).unique().tolist()
 
    folder = dataiku.Folder(FOLDER_ID)
 
    result_rows = []
    updates = {}  # var_key -> new subfolder name (string) to write back
 
    for flow in flow_names:
        var_key = VARIABLE_KEY_TEMPLATE.format(flow=flow)
        status = None
        max_name = None
        max_path = None
        max_dt = None
 
        if var_key not in project_vars:
            status = "missing_variable"
        else:
            reference_dt = parse_timestamp(project_vars[var_key])
            if reference_dt is None:
                status = "bad_reference_timestamp"
            else:
                subfolders = list_subfolders(folder, f"/{flow}")
                if subfolders is None:
                    status = "flow_folder_missing"
                else:
                    for name, full_path in subfolders:
                        ts = parse_timestamp(name)
                        if ts is None:
                            continue
                        if ts > reference_dt and (max_dt is None or ts > max_dt):
                            max_dt, max_name, max_path = ts, name, full_path
                    status = "ok" if max_path else "no_newer_subfolder"
 
        if status == "ok":
            updates[var_key] = max_name  # store the raw folder name, same format as before
            print(f"[OK] '{flow}': new max eligible subfolder -> {max_path} ({max_dt})")
        else:
            print(f"[INFO] '{flow}': status={status}")
 
        result_rows.append({
            "flow_name": flow,
            "variable_key": var_key,
            "max_timestamp_folder_path": max_path,
            "max_timestamp": max_dt.isoformat() if max_dt else None,
            "status": status,
        })
 
    result_df = pd.DataFrame(result_rows)
    print(result_df)

    # Rows that actually have new data -- this is what downstream steps consume.
    ready_df = result_df[result_df["status"] == "ok"][
        ["flow_name", "variable_key", "max_timestamp_folder_path", "max_timestamp"]
    ].reset_index(drop=True)

    if not OUTPUT_DATASET_NAME:
        raise Exception("OUTPUT_DATASET_NAME must be set -- downstream steps depend on it.")

    dataiku.Dataset(OUTPUT_DATASET_NAME).write_with_schema(ready_df)
    print(f"Wrote {len(ready_df)} ready-to-process flow(s) to '{OUTPUT_DATASET_NAME}'.")

    if ready_df.empty:
        print("No flow has data newer than its recorded timestamp -- stopping scenario, "
              "remaining steps will not run.")
        scenario.abort()
        return

    # Write the updated variables back to the project (only qualifying flows
    # change; anything left untouched keeps its previous value).
    project_vars.update(updates)
    variables["standard"] = project_vars
    project.set_variables(variables)

    print(f"Updated project variables: {updates}")
    print("Proceeding to next scenario step.")
 
