import os
from simple_salesforce import Salesforce
from utils.logger import get_logger

logger = get_logger("salesforce_service")

class SalesforceService:
    def __init__(self):
        self.sf = Salesforce(
            username=os.getenv("SF_USERNAME"),
            password=os.getenv("SF_PASSWORD"),
            security_token=os.getenv("SF_SECURITY_TOKEN"),
            domain=os.getenv("SF_DOMAIN", "login")
        )

    def query_all(self, soql: str):
        logger.info(f"Running SOQL: {soql}")
        data = self.sf.query_all(soql)
        records = data.get("records", [])
        cleaned = []
        for r in records:
            r.pop("attributes", None)
            cleaned.append(r)
        return cleaned