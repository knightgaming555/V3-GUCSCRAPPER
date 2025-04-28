import requests
import json
import sys


GITHUB_TOKEN = "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"
API_URL = "https://api.github.com/repos/knightgaming555/V3-GUCSCRAPPER/actions/workflows/prewarm_cache.yml/dispatches"
BRANCH = "main"


if len(sys.argv) != 3:
    print(f"Usage: python {sys.argv[0]} <GUC_USERNAME> <GUC_PASSWORD>", file=sys.stderr)
    print("WARNING: Passing password on command line is insecure.", file=sys.stderr)
    sys.exit(1)
guc_username = sys.argv[1]
guc_password = sys.argv[2]

# --- Prepare Request ---
headers = {
    "Accept": "application/vnd.github.v3+json",
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "X-GitHub-Api-Version": "2022-11-28",
    "Content-Type": "application/json",  # Added based on your info
}
payload = {
    "ref": BRANCH,
    "inputs": {
        "username": guc_username,
        "password": guc_password,  # Sending password as input
    },
}

# --- Send Request ---
try:
    print(f"Triggering workflow for user '{guc_username}'...")
    response = requests.post(API_URL, headers=headers, json=payload, timeout=15)
    response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

    if response.status_code == 204:
        print("Workflow triggered successfully (Status 204). Check GitHub Actions.")
    else:
        # Should be caught by raise_for_status, but as fallback
        print(f"Unexpected success status: {response.status_code}")
        print(response.text)

except requests.exceptions.RequestException as e:
    print(f"Error triggering workflow: {e}", file=sys.stderr)
    if hasattr(e, "response") and e.response is not None:
        print(f"Response Body: {e.response.text}", file=sys.stderr)
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred: {e}", file=sys.stderr)
    sys.exit(1)
