import os
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

def download_raw_files(container_name: str, files: list[str], target_dir: str) -> None:
    """Télécharge uniquement les fichiers CSV métiers spécifiés vers le dossier local."""
    account_name = os.getenv("account_name")
    account_key = os.getenv("account_key")
    
    if not account_name or not account_key:
        raise ValueError("Variables d'environnement 'account_name' ou 'account_key' manquantes.")
        
    os.makedirs(target_dir, exist_ok=True)
    
    client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net", 
        credential=account_key
    )
    container_client = client.get_container_client(container_name)
    
    for filename in files:
        local_path = os.path.join(target_dir, filename)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)  # ← ligne ajoutée
        blob_client = container_client.get_blob_client(filename)
        
        with open(local_path, "wb") as f:
            f.write(blob_client.download_blob().readall())

def get_spark_session() -> SparkSession:
    """Initialise et retourne la SparkSession localement."""
    return SparkSession.builder \
        .appName("TradeCorp_Ingestion") \
        .getOrCreate()

def read_csv_files(spark: SparkSession, data_dir: str, files: list[str]) -> dict[str, DataFrame]:
    """Charge les fichiers CSV téléchargés en DataFrames PySpark avec header et inferSchema."""
    return {
        f.replace(".csv", ""): spark.read.csv(
            os.path.join(data_dir, f), 
            header=True, 
            inferSchema=True
        )
        for f in files
    }

def read_reference() -> dict[str, DataFrame]:
    # Cibler le dossier data à la racine du projet
    target_dir = "/home/jovyan/data"
    
    files_to_download = [
        "reference/country_currency.csv",
        "reference/exchange_rates.json"
    ]
    
    download_raw_files("raw", files_to_download, target_dir)
    
    spark = get_spark_session()
    
    path_csv = os.path.join(target_dir, "reference", "country_currency.csv")
    path_json = os.path.join(target_dir, "reference", "exchange_rates.json")
    
    df_currency = spark.read.csv(path_csv, header=True, inferSchema=True)
    df_rates = spark.read.json(path_json)

    return {
        "country_currency": df_currency,
        "exchange_rates": df_rates
    }

