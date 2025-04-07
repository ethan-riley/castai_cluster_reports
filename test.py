from datetime import datetime, date

def simplify_version(version_str):
    """Return the simplified version by taking the first two numeric parts."""
    #parts = version_str.split(".")
    #return ".".join(parts[:2])
    version = ".".join(version_str.split("-")[0].split(".")[:2])
    #print(version)  # This will print: 1.31
    return version

def get_extended_support_data(provider):
    """
    Fetch the extended support data from endoflife.date for the given provider.
    For each provider the API returns a list of version objects. Each version object contains:
      • For EKS: 'cycle' (version), 'eol' (standard support end), 'extendedSupport' (extended support end)
      • For GKE: 'cycle', 'support' (standard support end), 'eol' (extended support end)
      • For AKS: 'cycle', 'eol' (standard support end), 'lts' (extended support end, if available)
    Returns the list of version objects (or an empty list on error).
    """
    import requests
    endpoints = {
        "EKS": "https://endoflife.date/api/amazon-eks.json",
        "GKE": "https://endoflife.date/api/google-kubernetes-engine.json",
        "AKS": "https://endoflife.date/api/azure-kubernetes-service.json"
    }
    url = endpoints.get(provider.upper())
    if not url:
        print(f"No endpoint defined for provider {provider}")
        return []
    try:
        resp = requests.get(url, headers={"Accept": "application/json"})
        data = resp.json()
    except Exception as e:
        print(f"Error fetching extended support data for {provider}: {e}")
        data = []
    return data

def determine_support_status(provider, version_str, support_data=None):
    """
    Determines if the given version is in standard support, extended support, or EOL.

    For provider:
      - EKS: uses 'eol' for standard support end and 'extendedSupport' for extended support end.
      - GKE: uses 'support' for standard support end and 'eol' for extended support end.
      - AKS: uses 'eol' for standard support end and 'lts' for extended support end.

    The function simplifies the input version (e.g. "1.31.6" becomes "1.31") and searches
    the support data for a matching version (comparing also in simplified form). Then, using the
    current date, it returns:
         "Standard" if today is on or before the standard support end date,
         "Extended" if today is after standard support but on or before extended support end date,
         "EOL" if today is after the extended support end date.
    If no match is found, it returns "Version not found" or "Unknown" if dates are missing.

    Optionally, you can pass pre-fetched support_data (a list of version objects) for the given provider.
    """
    # Simplify the version string
    simple_version = simplify_version(version_str)

    # If no support data is provided, fetch it
    if support_data is None:
        support_data = get_extended_support_data(provider)

    # Iterate over each version object
    for item in support_data:
        cycle = item.get("cycle", "")
        simple_cycle = simplify_version(cycle)
        if simple_cycle == simple_version:
            # For EKS, use 'eol' and 'extendedSupport'
            if provider.upper() == "EKS":
                std_date_str = item.get("eol", "")
                ext_date_str = item.get("extendedSupport", "")
            # For GKE, use 'support' and 'eol'
            elif provider.upper() == "GKE":
                std_date_str = item.get("support", "")
                ext_date_str = item.get("eol", "")
            # For AKS, use 'eol' and 'lts'
            elif provider.upper() == "AKS":
                std_date_str = item.get("eol", "")
                ext_date_str = item.get("lts", "")
            else:
                std_date_str = ""
                ext_date_str = ""

            try:
                std_date = datetime.fromisoformat(std_date_str).date() if std_date_str else None
            except Exception as e:
                print(f"Error parsing standard support date '{std_date_str}': {e}")
                std_date = None
            try:
                ext_date = datetime.fromisoformat(ext_date_str).date() if ext_date_str else None
            except Exception as e:
                print(f"Error parsing extended support date '{ext_date_str}': {e}")
                ext_date = None

            today = date.today()
            if std_date and today <= std_date:
                return "Standard"
            elif std_date and ext_date and std_date < today <= ext_date:
                return "Extended"
            elif ext_date and today > ext_date:
                return "EOL"
            else:
                return "Unknown"
    return "Version not found"

# Example usage:
if __name__ == "__main__":
    # For example, determine the support status for version "1.31.6" on EKS.
    provider = "EKS"
    version_input = "v1.32.0-eks-5ca49cb"
    status = determine_support_status(provider, version_input)
    print(f"For provider {provider} and version {version_input}, the support status is: {status}")

