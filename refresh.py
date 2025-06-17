import requests
import json
import msal
import time
from datetime import datetime


session = requests.Session()

# === Utility to acquire token ===
def get_access_token(client_id, client_secret, tenant_id):
   scope = ['https://analysis.windows.net/powerbi/api/.default']
   app = msal.ConfidentialClientApplication(
       client_id=client_id,
       client_credential=client_secret,
       authority=f'https://login.microsoftonline.com/{tenant_id}'
   )
   result = app.acquire_token_for_client(scopes=scope)
   if 'access_token' not in result:
       raise Exception("Failed to acquire access token")
   return result['access_token']


# === Method 1: Trigger Refresh ===
def trigger_refresh(workspace_id, dataset_id, client_id, client_secret, tenant_id):
   access_token = get_access_token(client_id, client_secret, tenant_id)


   refresh_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
   headers = {
       'Authorization': f'Bearer {access_token}',
       'Content-Type': 'application/json'
   }
   payload = {
       "type": "Full",
       "commitMode": "transactional",
       "maxParallelism": 2,
       "retryCount": 2,
       "timeout": "02:00:00",
       "objects": [
           {"table": "DimCustomer", "partition": "DimCustomer"},
           {"table": "DimDate"}
       ]
   }


   response = requests.post(refresh_url, headers=headers, data=json.dumps(payload))
   print(f"Trigger Refresh Response: {response.status_code} - {response.text}")


   if response.status_code != 202:
       raise Exception(f"Failed to trigger refresh: {response.status_code} - {response.text}")


   return True  # Just acknowledge it triggered


# === Method 2: Check Refresh Status Until Done ===
def wait_for_refresh_completion(workspace_id, dataset_id, client_id, client_secret, tenant_id, poll_interval=10):
   access_token = get_access_token(client_id, client_secret, tenant_id)


   status_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
   headers = {
       'Authorization': f'Bearer {access_token}',
       'Content-Type': 'application/json'
   }


   start_time = datetime.now()
   print(f"Started polling at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")


   while True:
       response = requests.get(status_url, headers=headers)
       if response.status_code != 200:
           raise Exception(f"Failed to fetch refresh status: {response.status_code} - {response.text}")


       refresh_data = response.json()
       latest_refresh = refresh_data.get('value', [])[0]


       status = latest_refresh.get('status', 'Unknown')
       print(f"Current Refresh Status: {status} at {datetime.now().strftime('%H:%M:%S')}")


       if status.lower() not in ["unknown", "inprogress"]:
           # If status is not 'InProgress' or 'Unknown', we're done
           end_time = datetime.now()
           total_time = (end_time - start_time).total_seconds()
           print(f"Finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
           print(f"Total Refresh Time: {total_time:.2f} seconds")
           print(f"Final Status: {status}")
           return status, start_time, end_time, total_time


       # Sleep and check again
       time.sleep(poll_interval)

# === Example Usage ===

# == Credentials ==
#client_secret = dbutils.secrets.get(scope="",key="")
client_id = ""
client_secret = ""
tenant_id = ""
workspace_id = ""
dataset_id = ""


# == Step 1: Trigger the refresh ==
trigger_refresh(workspace_id, dataset_id, client_id, client_secret, tenant_id)


# == Step 2: Keep polling until it's complete ==
status, start_time, end_time, total_time = wait_for_refresh_completion(
   workspace_id, dataset_id, client_id, client_secret, tenant_id, poll_interval=10
)


print(f"\nRefresh Completed with Status: {status}")
print(f"Started at: {start_time}")
print(f"Ended at: {end_time}")
print(f"Total Duration: {total_time} seconds")
