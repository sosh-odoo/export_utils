import requests
import logging
from typing import Dict, Any, List, Optional
logger = logging.getLogger(__name__)

class SalesforceAPI:
    def __init__(self, credentials):
        self.credentials = credentials
        self.access_token = None
        self.instance_url = None
        # self.api_version = "v59.0"
    
    def authenticate(self) -> bool:
        try:
            auth_url = "https://login.salesforce.com/services/oauth2/token"
            data = {
                "grant_type": "password",
                "client_id": self.credentials.get("client_id"),
                "client_secret": self.credentials.get("client_secret"),
                "username": self.credentials.get("username"),
                "password": f"{self.credentials.get('password')}{self.credentials.get('security_token')}",
            }
            
            response = requests.post(auth_url, data=data)
            
            if response.status_code == 200:
                result = response.json()
                self.access_token = result.get("access_token")
                self.instance_url = result.get("instance_url")
                logger.info(f"Authenticated with Salesforce: {self.instance_url}")
                return True
            else:
                logger.error(f"Failed to authenticate with Salesforce: {response.text}")
                return False
        except Exception as e:
            logger.error(f"Error authenticating with Salesforce: {str(e)}")
            return False
    
    def get_request_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
    
    def query(self, soql_query: str, batch_size: int, offset: int) -> List[Dict[str, Any]]:
        if not self.access_token:
            if not self.authenticate():
                return []
        
        try:
            paginated_query = f"{soql_query} LIMIT {batch_size} OFFSET {offset}"
            # url = f"{self.instance_url}/services/data/{self.api_version}/query/?q={soql_query}"
            url = f"{self.instance_url}/services/data/v59.0/query/?q={paginated_query}"
            
            response = requests.get(url, headers=self.get_request_headers())
            
            if response.status_code == 200:

                return response.json().get("records", [])
            
            elif response.status_code == 401:  # Token expired
                logger.info("Salesforce token expired. Re-authenticating...")
                if not self.authenticate():
                    return []
                # Retry the query
                return self.query(soql_query)
            else:
                logger.error(f"Salesforce API error: {response.text}")
                return []
        except Exception as e:
            logger.error(f"Error querying Salesforce: {str(e)}")
            return []
        