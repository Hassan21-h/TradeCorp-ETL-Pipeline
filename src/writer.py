import os
import glob
from azure.storage.blob import BlobServiceClient
from pyspark.sql import DataFrame


def write_to_parquet(df: DataFrame, local_path: str, mode: str = "overwrite") -> None:
    """Écrit le DataFrame en Parquet sur le disque local."""
    df.write.mode(mode).parquet(local_path)


def upload_directory_to_blob(local_dir: str, container_name: str, remote_prefix: str) -> None:
    """Upload tous les fichiers d'un dossier local vers Azure Blob Storage,
    en écrasant les anciens fichiers présents sous le même préfixe."""
    account_name = os.getenv("account_name")
    account_key = os.getenv("account_key")

    container_client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key,
    ).get_container_client(container_name)

    # Nettoyage de l'ancien contenu (équivalent du mode "overwrite")
    for blob in container_client.list_blobs(name_starts_with=remote_prefix):
        container_client.delete_blob(blob.name)

    # Upload des nouveaux fichiers générés par Spark
    for local_file in glob.glob(os.path.join(local_dir, "*")):
        filename = os.path.basename(local_file)
        with open(local_file, "rb") as f:
            container_client.upload_blob(f"{remote_prefix}/{filename}", f, overwrite=True)