import json
import logging
import requests
import msal
import time
from datetime import datetime


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def _validate_parameters(params: dict):
    """
    Validates that none of the required parameters are None, empty strings, or empty lists/dicts.

    :param params: Dictionary of parameters to validate
    :raises ValueError: If any parameter is missing or invalid
    """
    missing = []
    for key, value in params.items():
        if value is None:
            missing.append(key)
        elif isinstance(value, str) and not value.strip():
            missing.append(key)
        elif isinstance(value, (list, dict)) and not value:
            missing.append(key)

    if missing:
        raise ValueError(f"Missing or empty required parameters: {', '.join(missing)}")


class PowerBIRefreshManager:
    def __init__(self, client_id: str, client_secret: str, tenant_id: str, workspace_id: str, dataset_id: str, refresh_objects: list):
        """
        Initializes the PowerBIRefreshManager.

        :param client_id: Azure AD app client ID
        :param client_secret: Azure AD app client secret
        :param tenant_id: Azure tenant ID
        :param workspace_id: Power BI workspace ID
        :param dataset_id: Power BI dataset ID
        :param refresh_objects: List of objects to refresh (tables and partitions)
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.workspace_id = workspace_id
        self.dataset_id = dataset_id
        self.refresh_objects = refresh_objects

        _validate_parameters({
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "tenant_id": self.tenant_id,
            "workspace_id": self.workspace_id,
            "dataset_id": self.dataset_id,
            "refresh_objects": self.refresh_objects
        })

        self.access_token = self._get_access_token()

    def _get_access_token(self) -> str:
        """
        Acquires an access token using MSAL Confidential Client.

        :return: Access token string
        :raises Exception: If token acquisition fails
        """
        try:
            scope = ['https://analysis.windows.net/powerbi/api/.default']
            app = msal.ConfidentialClientApplication(
                client_id=self.client_id,
                client_credential=self.client_secret,
                authority=f'https://login.microsoftonline.com/{self.tenant_id}'
            )
            result = app.acquire_token_for_client(scopes=scope)
            if 'access_token' not in result:
                raise Exception(f"Failed to acquire token: {result.get('error_description', 'Unknown error')}")
            logger.info("Access token acquired successfully.")
            return result['access_token']
        except Exception as e:
            logger.error(f"Error acquiring access token: {str(e)}")
            raise

    def trigger_refresh(self):
        """
        Triggers a full dataset refresh on Power BI with specified objects.

        :raises Exception: If the refresh request fails
        """
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspace_id}/datasets/{self.dataset_id}/refreshes"
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

        payload = {
            "type": "Full",
            "commitMode": "transactional",
            "maxParallelism": 2,
            "retryCount": 2,
            "timeout": "02:00:00",
            "objects": self.refresh_objects
        }

        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            logger.info(f"Trigger Refresh Response: {response.status_code} - {response.text}")
            if response.status_code != 202:
                raise Exception("Refresh request failed")
        except Exception as e:
            logger.error(f"Error triggering refresh: {str(e)}")
            raise

    def wait_for_refresh_completion(self, poll_interval: int = 10):
        """
        Waits until the dataset refresh completes by polling Power BI API.

        :param poll_interval: Time in seconds between polling attempts
        :return: Tuple containing (status, start_time, end_time, total_time)
        :raises Exception: If refresh status check fails
        """
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspace_id}/datasets/{self.dataset_id}/refreshes?$top=1"
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

        start_time = datetime.now()
        logger.info(f"Started polling at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        while True:
            try:
                response = requests.get(url, headers=headers)
                if response.status_code != 200:
                    raise Exception(f"Status check failed: {response.status_code} - {response.text}")

                refresh_data = response.json()
                latest_refresh = refresh_data.get('value', [{}])[0]
                status = latest_refresh.get('status', 'Unknown')

                logger.info(f"Current Refresh Status: {status} at {datetime.now().strftime('%H:%M:%S')}")

                if status.lower() not in ["unknown", "inprogress"]:
                    end_time = datetime.now()
                    total_time = (end_time - start_time).total_seconds()
                    logger.info(f"Finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    logger.info(f"Total Refresh Time: {total_time:.2f} seconds")
                    return status, start_time, end_time, total_time

                time.sleep(poll_interval)

            except Exception as e:
                logger.error(f"Error checking refresh status: {str(e)}")
                raise