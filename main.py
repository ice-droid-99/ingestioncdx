from dotenv import load_dotenv
from services.ingestion_service import IngestionService

def main():
    # In ECS, env vars will already be set; load_dotenv is harmless locally.
    load_dotenv()
    IngestionService().run()

if __name__ == "__main__":
    main()