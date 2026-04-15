from dotenv import load_dotenv
from services.ingestion_service import IngestionService

def main():
    load_dotenv()  # harmless in ECS, useful locally
    IngestionService().run()

if __name__ == "__main__":
    main()