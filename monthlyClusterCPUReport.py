#!/usr/bin/env python3
import os
import sys
import subprocess
import pandas as pd
import datetime
import calendar

# -------------------------
# Helper Functions
# -------------------------
def get_month_range(year, month):
    """Return start_time and end_time strings for the given month in nanosecond format."""
    start = datetime.datetime(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end = datetime.datetime(year, month, last_day, 23, 59, 59, 999999)
    start_str = start.strftime("%Y-%m-%dT%H:%M:%S.000000000Z")
    end_str = end.strftime("%Y-%m-%dT%H:%M:%S.000000000Z")
    return start_str, end_str

# Dummy function to represent fetching monthly CPU info.
def fetch_cluster_info(api_key, org_id, csv_path):
    # Read the cluster_details.csv and sort by Connected Date
    df = pd.read_csv(csv_path)
    if "Connected Date" in df.columns:
        df["Connected Date"] = pd.to_datetime(df["Connected Date"], errors='coerce')
        df.sort_values(by="Connected Date", inplace=True)
    else:
        print("Connected Date column not found in CSV.", flush=True)
        sys.exit(1)
    
    # Insert your actual logic here to compute monthly CPU report.
    print(f"Generating monthly CPU report for {len(df)} clusters...", flush=True)
    
    # For demonstration, we simply output the sorted DataFrame.
    output_csv = os.path.join(os.path.dirname(csv_path), "monthly_cpu_report.csv")
    df.to_csv(output_csv, index=False)
    print(f"Monthly CPU report saved to {output_csv}", flush=True)

def process_org(selected_org, org_row):
    api_key = org_row["key"]
    org_dir_local = os.path.join("outputs", selected_org.replace(" ", "_"))
    global org_dir
    org_dir = org_dir_local
    os.makedirs(org_dir, exist_ok=True)
    csv_dir = os.path.join(org_dir, "csv")
    details_csv = os.path.join(csv_dir, "cluster_details.csv")
    if not os.path.exists(details_csv):
        print(f"cluster_details.csv not found for {selected_org}. Running orgClusterDetails.py...", flush=True)
        try:
            if save_json == "on":
                subprocess.run(["python", "orgClusterDetails.py", selected_org, "on"], check=True)
            else:
                subprocess.run(["python", "orgClusterDetails.py", selected_org], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running orgClusterDetails.py for {selected_org}: {e}", flush=True)
            sys.exit(1)
        if not os.path.exists(details_csv):
            print("Failed to generate cluster_details.csv.", flush=True)
            sys.exit(1)
    else:
        print(f"Found cluster_details.csv for {selected_org}.", flush=True)
    os.makedirs(os.path.join(org_dir, "json"), exist_ok=True)
    fetch_cluster_info(api_key, org_row["org_id"], details_csv)

def main():
    global save_json
    if len(sys.argv) < 2:
        print("Usage: python orgClusterDetails.py <Organization | all> <on> (If you want to save resulting jsons)", flush=True)
        sys.exit(1)
    elif len(sys.argv) == 2:
        save_json="off"
    elif len(sys.argv) == 3:
        save_json = sys.argv[2].strip()
    selected_arg = " ".join(sys.argv[1:]).strip()
    try:
        orgs_df = pd.read_csv("orgs.csv")
    except Exception as e:
        print(f"Error loading orgs.csv: {e}", flush=True)
        sys.exit(1)
    if selected_arg.lower() == "all":
        for idx, org_row in orgs_df.iterrows():
            selected_org = org_row["org"]
            print(f"Processing organization: {selected_org}", flush=True)
            process_org(selected_org, org_row)
    else:
        try:
            org_row = orgs_df[orgs_df["org"] == selected_arg].iloc[0]
        except Exception as e:
            print(f"Organization '{selected_arg}' not found: {e}", flush=True)
            sys.exit(1)
        process_org(selected_arg, org_row)

if __name__ == "__main__":
    main()