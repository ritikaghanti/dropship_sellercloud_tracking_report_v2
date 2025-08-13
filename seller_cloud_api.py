import requests
from kramer_functions import AzureSecrets

class SellerCloudAPI():
    """A class to interact with the SellerCloud API."""

    def __init__(self):
        self.base_url = "https://krameramerica.api.sellercloud.us/rest/api/"
        self.access_token = self.get_token()
        self.session = self._create_session()
    
    def get_order(self, order_id: str):
        """GET a single order by ID from SellerCloud."""
        url = self.base_url + "Orders"   # adjust if your endpoint differs
        return self.session.get(f"{url}/{order_id}", timeout=30)
    
    def get_token(self):
        """Retrieves the SellerCloud API access token."""
        self.secrets = AzureSecrets()
        sc_username = self.secrets.get_secret("sc-username-dan")
        sc_password = self.secrets.get_secret("sc-password-dan")
        data = {
            "Username": sc_username,
            "Password": sc_password,
        }
        url = self.base_url + "token"
        response = requests.post(url, json=data)
        if response.status_code != 200:
            raise Exception(f"Failed to get SellerCloud API token: {response.text}")
        return response.json()["access_token"]

    def _create_session(self):
        """Creates a session with default headers."""
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
            }
        )
        return session