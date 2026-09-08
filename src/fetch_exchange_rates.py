import os
import json
import logging
import requests
from azure.storage.blob import BlobServiceClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

recherche = "https://api.exchangerate-api.com/v4/latest/USD"

response = requests.get(recherche).text

def upload_json():
    account_name = os.getenv("account_name")
    account_key = os.getenv("account_key")
    container_name = "raw"

    data = json.loads(response)
    logger.info(f"{len(data['rates'])} devises récupérées depuis l'API.")

    container_client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    ).get_container_client(container_name)

    blob_path = "reference/exchange_rates.json"
    blob_client = container_client.get_blob_client(blob_path)

    blob_client.upload_blob(response, overwrite=True)

    logger.info(f"Fichier envoyé dans : {container_name}/{blob_path}")

if __name__ == "__main__":
    upload_json()