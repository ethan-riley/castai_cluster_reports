#!/usr/bin/env python3
import os
import sys
import re
from xml.dom.minidom import Attr
import requests
import pandas as pd
import datetime
import json
import math
import statistics

# -------------------------
# Helper Functions
# -------------------------
def get_cluster_ids(api_key, org_id):
    print("Getting Organization Clusters", flush=True)
    url = "https://api.cast.ai/v1/cost-reports/organization/clusters/summary"
    headers = {"accept": "application/json", "X-API-Key": api_key}
    resp = requests.get(url, headers=headers)
    try:
        data = resp.json()
    except Exception as e:
        print(f"Error decoding cluster IDs: {e}", flush=True)
        data = {}
    os.makedirs(os.path.join(org_dir, "json"), exist_ok=True)
    if save_json == "on":
        with open(os.path.join(org_dir, "json", "get_cluster_ids.json"), 'w') as file:
            json.dump(data, file, indent=4)
    offerings = {}
    for item in data.get("items", []):
        cluster_id = item.get("clusterId")
        if cluster_id:
            offerings[cluster_id] = {
                "nodeCountOnDemand": int(item.get("nodeCountOnDemand", "0")),
                "nodeCountSpot": int(item.get("nodeCountSpot", "0")),
                "nodeCountOnDemandCastai": int(item.get("nodeCountOnDemandCastai", "0")),
                "nodeCountSpotCastai": int(item.get("nodeCountSpotCastai", "0")),
                "nodeCountSpotFallbackCastai": int(item.get("nodeCountSpotFallbackCastai", "0"))
            }
    return offerings

def get_cluster_details(api_key, cluster_id):
    print(f"Getting Cluster Details for cluster {cluster_id}", flush=True)
    url = f"https://api.cast.ai/v1/kubernetes/external-clusters/{cluster_id}"
    headers = {"accept": "application/json", "X-API-Key": api_key}
    resp = requests.get(url, headers=headers)
    try:
        data = resp.json()
    except Exception as e:
        print(f"Error decoding cluster details for {cluster_id}: {e}", flush=True)
        data = {}
    os.makedirs(os.path.join(org_dir, "json"), exist_ok=True)
    if save_json == "on":
        with open(os.path.join(org_dir, "json", f"get_cluster_details_{cluster_id}.json"), 'w') as file:
            json.dump(data, file, indent=4)
    return data

def compute_resource_offering(offering):
    on_demand = offering.get("nodeCountOnDemand", 0) + offering.get("nodeCountOnDemandCastai", 0)
    spot = offering.get("nodeCountSpot", 0) + offering.get("nodeCountSpotCastai", 0)
    fallback = offering.get("nodeCountSpotFallbackCastai", 0)
    total = on_demand + spot + fallback
    if total == 0:
        return ""
    on_demand_pct = round((on_demand / total) * 100)
    spot_pct = round((spot / total) * 100)
    fallback_pct = round((fallback / total) * 100)
    return f"OnDemand {on_demand_pct}% - Spot {spot_pct}% - Fallback {fallback_pct}%"

def get_evictor_status(api_key, cluster_id):
    post_url = f"https://api.cast.ai/v1/kubernetes/clusters/{cluster_id}/evictor-config"
    headers = {"accept": "application/json", "X-API-Key": api_key, "Content-Type": "application/json"}
    post_resp = requests.post(post_url, headers=headers, json={})
    try:
        post_data = post_resp.json()
    except Exception as e:
        print(f"Error decoding evictor config POST for {cluster_id}: {e}", flush=True)
        post_data = {}
    if save_json == "on":
        post_file = os.path.join(org_dir, "json", f"post_evictor_config_{cluster_id}.json")
        with open(post_file, 'w') as f:
            json.dump(post_data, f, indent=4)
    if not post_data.get("isReady", False):
        return "Uninstalled"
    get_url = f"https://api.cast.ai/v1/kubernetes/clusters/{cluster_id}/evictor-advanced-config"
    get_resp = requests.get(get_url, headers={"accept": "application/json", "X-API-Key": api_key})
    try:
        get_data = get_resp.json()
    except Exception as e:
        print(f"Error decoding evictor advanced config GET for {cluster_id}: {e}", flush=True)
        get_data = {}
    if save_json == "on":
        get_file = os.path.join(org_dir, "json", f"get_evictor_advanced_config_{cluster_id}.json")
        with open(get_file, 'w') as f:
            json.dump(get_data, f, indent=4)
    if "evictionConfig" in get_data:
        if not get_data["evictionConfig"]:
            return "Installed (Basic)"
        else:
            return "Installed (Advanced)"
    return ""

def get_cluster_settings(api_key, cluster_id):
    url = f"https://api.cast.ai/v1/kubernetes/clusters/{cluster_id}/settings"
    headers = {"X-API-Key": api_key, "accept": "application/json"}
    resp = requests.get(url, headers=headers)
    try:
        data = resp.json()
    except Exception as e:
        print(f"Error decoding settings for cluster {cluster_id}: {e}", flush=True)
        data = {}
    if save_json == "on":
        file_path = os.path.join(org_dir, "json", f"get_cluster_settings_{cluster_id}.json")
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
    return data

def get_rebalancing_plans(api_key, cluster_id):
    url = f"https://api.cast.ai/v1/kubernetes/clusters/{cluster_id}/rebalancing-plans?limit=10"
    headers = {"X-API-Key": api_key, "accept": "application/json"}
    resp = requests.get(url, headers=headers)
    try:
        data = resp.json()
    except Exception as e:
        print(f"Error decoding rebalancing plans for cluster {cluster_id}: {e}", flush=True)
        data = {}
    if save_json == "on":
        file_path = os.path.join(org_dir, "json", f"get_rebalancing_plans_{cluster_id}.json")
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
    for plan in data.get("items", []):
        if plan.get("status", "").lower() == "finished":
            return "Yes"
    return "No"

def get_woop_enabled_percent(api_key, cluster_id):
    url = f"https://api.cast.ai/v1/workload-autoscaling/clusters/{cluster_id}/workloads-summary?includeCosts=true"
    headers = {"accept": "application/json", "X-API-Key": api_key}
    resp = requests.get(url, headers=headers)
    try:
        data = resp.json()
    except Exception as e:
        print(f"Error decoding workloads-summary for cluster {cluster_id}: {e}", flush=True)
        data = {}
    if save_json == "on":
        file_path = os.path.join(org_dir, "json", f"get_workloads_summary_{cluster_id}.json")
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
    total = data.get("totalCount", 0)
    optimized = data.get("optimizedCount", 0)
    try:
        ratio = float(optimized) / float(total) if float(total) > 0 else 0
    except:
        ratio = 0
    return f"{ratio*100:.2f}%"

def detect_environment(cluster_name, tag_env=""):
    name = cluster_name.lower()
    prod_patterns = [r'\bprod\b', r'\bproduction\b', r'\bprd\b', r'\bproduccion\b', r'\bp\b', r'\bpd\b']
    staging_patterns = [r'\bqa\b', r'\bqas\b', r'\buat\b', r'\bquality[- ]?assurance\b', r'\bqat\b', r'\bq\b', r'\btest\b', r'\bstaging\b']
    dev_patterns = [r'\bdev\b', r'\bdesa\b', r'\bdv\b', r'\bde\b', r'\bdevelopment\b', r'\bdesarrollo\b', r'\bdes\b']
    integration_patthers = [r'\bcd\b', r'\bci\b', r'\bargo\b', r'\bjenkins\b']
    for pattern in prod_patterns:
        if re.search(pattern, name, re.IGNORECASE):
            return "Production"
    for pattern in staging_patterns:
        if re.search(pattern, name, re.IGNORECASE):
            return "Staging"
    for pattern in dev_patterns:
        if re.search(pattern, name, re.IGNORECASE):
            return "Development"
    for pattern in integration_patthers:
        if re.search(pattern, name, re.IGNORECASE):
            return "Integration"
    if tag_env:
        return tag_env.upper()
    return "unknown"

def get_nodes_managed(api_key, cluster_id, provider_name):
    """
    Calls the nodes endpoint for a given cluster and calculates the percentage
    of nodes managed by CastAI, by the provider (using provider_name), and, if any,
    by Karpenter.
    Returns a string formatted like:
      "CastAI = 20.00%; EKS = 70.00%; Karpenter = 10.00%"
    """
    url = f"https://api.cast.ai/v1/kubernetes/external-clusters/{cluster_id}/nodes?nodeStatus=node_status_unspecified&lifecycleType=lifecycle_type_unspecified"
    headers = {"X-API-Key": api_key, "accept": "application/json"}
    resp = requests.get(url, headers=headers)
    
    try:
        data = resp.json()
    except Exception as e:
        print(f"Error decoding nodes for cluster {cluster_id}: {e}", flush=True)
        data = {}
    
    # Make sure the directory exists
    #org_dir = os.path.join(os.getcwd(), "json")
    os.makedirs(org_dir, exist_ok=True)
    os.makedirs(os.path.join(org_dir, "json"), exist_ok=True)
    if save_json == "on":    
        file_path = os.path.join(org_dir, "json", f"nodes_{cluster_id}.json")
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
    
    items = data.get("items", [])
    total_nodes = len(items)
    
    if total_nodes == 0:
        return "No nodes found"
    
    # Use the uppercase version of provider_name as a key
    provider_key = provider_name.upper()
    if provider_key == "ANYWHERE":
        value = "Nodes 100% managed by Kubernetes Controller"
        return value
    
    manager_counts = {
        "CastAI": 0,
        "Karpenter": 0,
        provider_key: 0
    }
    
    # Lists to store CPU and memory usage percentages for each manager
    cpu_usage_by_manager = {
        "CastAI": [],
        "Karpenter": [],
        provider_key: []
    }
    
    mem_usage_by_manager = {
        "CastAI": [],
        "Karpenter": [],
        provider_key: []
    }
    
    for item in items:
        labels = item.get("labels", {})
        resources = item.get("resources", {})
        
        # Determine node manager
        if "provisioner.cast.ai/managed-by" in labels and labels["provisioner.cast.ai/managed-by"] == "cast.ai":
            manager = "CastAI"
            manager_counts["CastAI"] += 1
        elif "karpenter.sh/registered" in labels and labels["karpenter.sh/registered"] == "true":
            manager = "Karpenter"
            manager_counts["Karpenter"] += 1
        else:
            manager = provider_key
            manager_counts[provider_key] += 1
        if "failure-domain.beta.kubernetes.io/region" in labels:
            region = labels["failure-domain.beta.kubernetes.io/region"]
        
        # Calculate CPU usage percentage
        cpu_capacity = resources.get("cpuCapacityMilli", 0)
        cpu_requests = resources.get("cpuRequestsMilli", 0)
        
        if cpu_capacity > 0:
            cpu_usage_pct = (cpu_requests / cpu_capacity) * 100
            cpu_usage_by_manager[manager].append(cpu_usage_pct)
        
        # Calculate memory usage percentage
        mem_capacity = resources.get("memCapacityMib", 0)
        mem_requests = resources.get("memRequestsMib", 0)
        
        if mem_capacity > 0:
            mem_usage_pct = (mem_requests / mem_capacity) * 100
            mem_usage_by_manager[manager].append(mem_usage_pct)
    
    # Calculate average usage percentages
    avg_cpu_usage = {}
    avg_mem_usage = {}
    
    for manager in manager_counts.keys():
        if cpu_usage_by_manager[manager]:
            avg_cpu_usage[manager] = statistics.mean(cpu_usage_by_manager[manager])
        else:
            avg_cpu_usage[manager] = 0
            
        if mem_usage_by_manager[manager]:
            avg_mem_usage[manager] = statistics.mean(mem_usage_by_manager[manager])
        else:
            avg_mem_usage[manager] = 0
    
    result_parts = []
    
    # Build formatted output for each manager with nodes
    for manager in manager_counts.keys():
        if manager_counts[manager] > 0:
            node_percentage = (manager_counts[manager] / total_nodes) * 100
            manager_result = (
                f"{manager}: {manager_counts[manager]}/{total_nodes} nodes ({node_percentage:.1f}%), "
                f"{avg_cpu_usage[manager]:.1f}% CPU usage, "
                f"{avg_mem_usage[manager]:.1f}% memory usage"
            )
            result_parts.append(manager_result)
    
    # Join all results with semicolon
    # print(result_parts)
    return "; ".join(result_parts)

def get_cpu_count(api_key, cluster_id):
    """
    Returns the total CPU capacity (in millicores) provided by all nodes in the cluster.
    It uses the external-clusters nodes endpoint and sums up the value of 
    resource.cpuCapacityMilli for each node.
    """
    url = f"https://api.cast.ai/v1/kubernetes/external-clusters/{cluster_id}/nodes?nodeStatus=node_status_unspecified&lifecycleType=lifecycle_type_unspecified"
    headers = {"X-API-Key": api_key, "accept": "application/json"}
    resp = requests.get(url, headers=headers)
    try:
        data = resp.json()
    except Exception as e:
        print(f"Error decoding nodes for cluster {cluster_id}: {e}", flush=True)
        data = {}
    # Save the JSON response if needed
    os.makedirs(os.path.join(org_dir, "json"), exist_ok=True)
    if save_json == "on":
        file_path = os.path.join(org_dir, "json", f"nodes_{cluster_id}.json")
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
    total_cpu = 0.0
    for node in data.get("items", []):
        try:
            cpu = float(node.get("resources", {}).get("cpuCapacityMilli", 0))
        except Exception as e:
            print(f"Error processing cpuCapacityMilli for node in cluster {cluster_id}: {e}", flush=True)
            cpu = 0.0
        total_cpu += cpu
    total_cpu = total_cpu/1000
    total_cpu = round(total_cpu, None)
    return total_cpu

def extract_cluster_info(cluster_id, details, offerings, api_key, schedule_map):
    info = {}
    info["ClusterID"] = cluster_id
    info["Cluster Name"] = details.get("name", "")
    provider = details.get("providerType", "")
    info["Provider"] = provider.upper() if provider else ""

    is_phase2 = details.get("isPhase2")
    if is_phase2 in [True, "true", "True"]:
        info["Phase 1"] = "Yes"
        info["Phase 2"] = "Yes"
    else:
        info["Phase 1"] = "Yes"
        info["Phase 2"] = "No"
    
    info["WOOP Enabled"] = "Yes" if get_woop_enabled_percent(api_key, cluster_id) != "0.00%" else "No"

    if cluster_id in offerings:
        info["Resource Offering"] = compute_resource_offering(offerings[cluster_id])
    else:
        info["Resource Offering"] = details.get("resourceOffering", "")
    
    info["First Rebalance"] = get_rebalancing_plans(api_key, cluster_id)
    
    info["Special Considerations"] = details.get("specialConsiderations", "")
    
    if provider.lower() == "anywhere":
        date_str = details.get("createdAt", "")
    else:
        date_str = details.get("firstOperationAt", "")
    if date_str:
        try:
            info["Connected Date"] = datetime.datetime.fromisoformat(date_str[:10]).date().isoformat()
        except Exception as e:
            print(f"Error parsing Connected Date for cluster {cluster_id}: {e}", flush=True)
            info["Connected Date"] = ""
    else:
        info["Connected Date"] = ""
    
    tags = details.get("tags", {})
    info["Environment"] = detect_environment(details.get("name", ""), tags.get("Environment", ""))
    
    info["Evictor"] = get_evictor_status(api_key, cluster_id)
    
    if cluster_id in schedule_map:
        info["Scheduled Rebalance"] = "Yes: " + "; ".join(schedule_map[cluster_id])
    else:
        info["Scheduled Rebalance"] = ""
    
    info["Node Templates Review"] = details.get("nodeTemplatesReview", "")
    info["WOOP enabled %"] = get_woop_enabled_percent(api_key, cluster_id)
    info["Kubernetes version"] = details.get("kubernetesVersion", "")
    
    settings = get_cluster_settings(api_key, cluster_id)
    karp_val = settings.get("karpenterInstalled", False)
    if isinstance(karp_val, bool):
        info["KarpenterInstalled"] = "Yes" if karp_val else "No"
    else:
        info["KarpenterInstalled"] = str(karp_val)
    
    # New column "Nodes Managed"
    info["Nodes Managed"] = get_nodes_managed(api_key, cluster_id, provider)

    #Get Region
    if provider.lower() == "anywhere":
        info["Region"] = get_anywhere_region(api_key, cluster_id)
    elif provider.lower() == "eks" and "gke" and "aks":
        regionlabels = details.get("region")
        info["Region"] = regionlabels.get("name")
    else:
        info["Region"] = "Unknown"
    info["CPU Count"] = get_cpu_count(api_key,cluster_id)
    return info

def get_all_rebalancing_schedules(api_key):
    url = "https://api.cast.ai/v1/rebalancing-schedules"
    headers = {"accept": "application/json", "X-API-Key": api_key}
    resp = requests.get(url, headers=headers)
    try:
        data = resp.json()
    except Exception as e:
        print(f"Error decoding rebalancing schedules: {e}", flush=True)
        data = {}
    if save_json == "on":
        file_path = os.path.join(org_dir, "json", "get_rebalancing_schedules.json")
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
    schedule_map = {}
    for schedule in data.get("schedules", []):
        cron = schedule.get("schedule", {}).get("cron", "")
        next_trigger = schedule.get("nextTriggerAt", "")
        schedule_desc = f"Cron: {cron}, Next: {next_trigger}"
        for job in schedule.get("jobs", []):
            cid = job.get("clusterId", "")
            if cid:
                schedule_map.setdefault(cid, []).append(schedule_desc)
    return schedule_map

def get_anywhere_region(api_key, cluster_id):
    urldos = f"https://api.cast.ai/v1/kubernetes/external-clusters/{cluster_id}/nodes?nodeStatus=node_status_unspecified&lifecycleType=lifecycle_type_unspecified"
    headersdos = {"X-API-Key": api_key, "accept": "application/json"}
    respdos = requests.get(urldos, headers=headersdos)
    try:
        datados = respdos.json()
    except Exception as e:
        print(f"Error decoding nodes for cluster {cluster_id}: {e}", flush=True)
        datados = {}
    
    # Make sure the directory exists
    #org_dir = os.path.join(os.getcwd(), "json")
    os.makedirs(org_dir, exist_ok=True)
    os.makedirs(os.path.join(org_dir, "json"), exist_ok=True)
    if save_json == "on":    
        file_path = os.path.join(org_dir, "json", f"nodes_{cluster_id}.json")
        with open(file_path, "w") as f:
            json.dump(datados, f, indent=4)
    
    items = datados.get("items", [])
    total_nodes = len(items)

    if total_nodes == 0:
        region =  "No region data found"
    for item in items:
        labels = item.get("labels", {})
        if "failure-domain.beta.kubernetes.io/region" in labels:
            region = labels["failure-domain.beta.kubernetes.io/region"]
        else:
            region = "Unknown"
    return region

def fetch_cluster_info(api_key, org_id):
    offerings = get_cluster_ids(api_key, org_id)
    schedule_map = get_all_rebalancing_schedules(api_key)
    cluster_ids = list(offerings.keys())
    if not cluster_ids:
        print("No clusters found.", flush=True)
        return
    all_cluster_info = []
    for cluster_id in cluster_ids:
        details = get_cluster_details(api_key, cluster_id)
        cluster_info = extract_cluster_info(cluster_id, details, offerings, api_key, schedule_map)
        all_cluster_info.append(cluster_info)
    df = pd.DataFrame(all_cluster_info)
    df["Connected Date"] = pd.to_datetime(df["Connected Date"], errors='coerce')
    df.sort_values(by="Connected Date", inplace=True)
    cols = ["ClusterID", "Cluster Name", "Provider", "Region", "Phase 1", "Phase 2", "WOOP Enabled",
            "Resource Offering", "First Rebalance", "Special Considerations", "Connected Date",
            "Environment", "Evictor", "Scheduled Rebalance", "Node Templates Review",
            "WOOP enabled %", "Kubernetes version", "KarpenterInstalled", "CPU Count", "Nodes Managed"]
    df = df.reindex(columns=cols)
    csv_path = os.path.join(org_dir, "csv", "cluster_details.csv")
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=False)
    print(f"Cluster details saved to {csv_path}")

def process_org(selected_org, org_row):
    api_key = org_row["key"]
    org_dir_local = os.path.join("outputs", selected_org.replace(" ", "_"))
    global org_dir
    org_dir = org_dir_local
    os.makedirs(org_dir, exist_ok=True)
    os.makedirs(os.path.join(org_dir, "json"), exist_ok=True)
    fetch_cluster_info(api_key, org_row["org_id"])

def main():
    global save_json
    if len(sys.argv) < 2:
        print("Usage: python orgClusterDetails.py <Organization | all> <on> (If you want to save resulting jsons)", flush=True)
        sys.exit(1)
    elif len(sys.argv) == 2:
        save_json="off"
    elif len(sys.argv) == 3:
        save_json = sys.argv[2].strip()

    arg = sys.argv[1].strip()        
    try:
        orgs_df = pd.read_csv("orgs.csv")
    except Exception as e:
        print(f"Error loading orgs.csv: {e}", flush=True)
        sys.exit(1)
    if arg.lower() == "all":
        for idx, org_row in orgs_df.iterrows():
            selected_org = org_row["org"]
            print(f"Processing organization: {selected_org}", flush=True)
            process_org(selected_org, org_row)
    else:
        try:
            org_row = orgs_df[orgs_df["org"] == arg].iloc[0]
        except Exception as e:
            print(f"Organization '{arg}' not found: {e}", flush=True)
            sys.exit(1)
        process_org(arg, org_row)

if __name__ == "__main__":
    main()