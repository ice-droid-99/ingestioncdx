from dotenv import load_dotenv
from services.ingestion_service import IngestionService

def main():
    load_dotenv()
    service = IngestionService(
        config_path="config/ingestion_config.json",
        output_root="output",
        logs_root="logs",
        state_root="state"
    )
    service.run()

if __name__ == "__main__":
    main()