from pathlib import Path
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.oauth2 import service_account


class GA4Client:
    """Client for interacting with Google Analytics 4 Data API."""

    def __init__(self, service_account_path: str, property_id: str):
        self.property_id = property_id
        self.service_account_path = service_account_path
        self.client = self._create_client()
        self.reports = {}

    def _create_client(self) -> BetaAnalyticsDataClient:
        """Create a BetaAnalyticsDataClient object."""
        path = Path(self.service_account_path)
        if not path.is_file():
            raise FileNotFoundError("Service account key file not found.")
        else:
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    path, scopes=["https://www.googleapis.com/auth/analytics.readonly"]
                )
                return BetaAnalyticsDataClient(credentials=credentials)
            except ValueError as e:
                raise ValueError(f"Invalid credentials file: {e}")
