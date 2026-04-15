import logging
import os

from simple_salesforce import Salesforce

from app.orchestrator.ingestion_orchestrator import IngestionOrchestrator
from app.services.config_service import ConfigService
from app.services.metadata_service import MetadataService
from app.services.s3_service import S3Service
from app.services.salesforce_service import SalesforceService
from app.services.schema_registry_service import SchemaRegistryService
from app.services.watermark_service import WatermarkService
from app.services.writer_service import WriterService

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


def build_sf_client() -> Salesforce:
    return Salesforce(
        username=os.environ["SALESFORCE_USERNAME"],
        password=os.environ["SALESFORCE_PASSWORD"],
        security_token=os.environ["SALESFORCE_SECURITY_TOKEN"],
        domain=os.getenv("SALESFORCE_DOMAIN", "login"),
    )


def main() -> None:
    config_uri = os.getenv("CONFIG_URI", "s3://demo445/config/ingestion_config.json")
    output_prefix_uri = os.getenv("OUTPUT_PREFIX_URI", "s3://demo445/output")
    schema_registry_uri = os.getenv("SCHEMA_REGISTRY_URI", "s3://demo445/log/schema_registry.json")
    watermark_prefix_uri = os.getenv("WATERMARK_PREFIX_URI", "s3://demo445/log/watermarks")

    s3 = S3Service()
    sf_client = build_sf_client()

    config_service = ConfigService(s3, config_uri)
    metadata_service = MetadataService(sf_client)
    salesforce_service = SalesforceService(sf_client)
    schema_registry_service = SchemaRegistryService(s3, schema_registry_uri)
    writer_service = WriterService(s3, output_prefix_uri)
    watermark_service = WatermarkService(s3, watermark_prefix_uri)

    orchestrator = IngestionOrchestrator(
        config_service=config_service,
        metadata_service=metadata_service,
        salesforce_service=salesforce_service,
        schema_registry_service=schema_registry_service,
        writer_service=writer_service,
        watermark_service=watermark_service,
    )
    orchestrator.run()


if __name__ == "__main__":
    main()