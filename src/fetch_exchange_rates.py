import os
import requests
from azure.storage.blob import BlobServiceClient

recherche = "https://api.exchangerate-api.com/v4/latest/USD"

# 1. On récupère le texte brut du JSON directement (pas besoin de .json() ni de json.dumps)
response = requests.get(recherche).text

def upload_json():
    account_name = os.getenv("account_name")
    account_key = os.getenv("account_key")
    container_name = "raw" 

    # 2. Connexion au conteneur Azure
    container_client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    ).get_container_client(container_name)

    # 3. Ciblage du chemin dans le dossier 'reference'
    blob_path = "reference/exchange_rates.json"
    blob_client = container_client.get_blob_client(blob_path)

    # 4. Envoi direct du texte reçu de l'API
    blob_client.upload_blob(response, overwrite=True)

    print(f"Fichier envoyé dans : {container_name}/{blob_path}")

if __name__ == "__main__":
    upload_json()

