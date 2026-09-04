import pytest
from pyspark.sql import SparkSession

from utils import add_sous_total, clean_customers, clean_orders
from enrichment import enrich_with_currency


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("pytest-spark").master("local[1]").getOrCreate()
    yield spark
    spark.stop()


def test_add_sous_total(spark):
    data = [(1, 10, 20.0, 0.0), (2, 5, 5.0, 0.1)]
    df = spark.createDataFrame(data, ["order_id", "quantite", "prix_unitaire", "discount"])

    resultats = [row["sous_total"] for row in add_sous_total(df).orderBy("order_id").collect()]

    assert resultats == [200.0, 22.5]


def test_clean_customers(spark):
    data = [
        (1, "tradecorp", " frederic ", " ouzbekistan ", "title", "addr", "city", "region"),
        (2, "corptrade", " alice ", " moldavie ", "title", "addr", "city", "region"),
    ]
    columns = ["customer_id", "company_name", "contact_name", "country", "contact_title", "address", "city", "region"]
    df = spark.createDataFrame(data, columns)

    results = clean_customers(df).select("contact_name", "country").collect()

    assert results[0]["contact_name"] == "Frederic"
    assert results[0]["country"] == "OUZBEKISTAN"
    assert results[1]["contact_name"] == "Alice"
    assert results[1]["country"] == "MOLDAVIE"


def test_clean_orders(spark):
    data = [
        (101, "2023-01-01", "2023-01-05", "2023-01-04", 10.0),  # shipped_date renseignée -> gardée
        (102, "2023-01-02", "2023-01-06", None, 20.0),          # shipped_date null -> supprimée
    ]
    columns = ["order_id", "order_date", "required_date", "shipped_date", "freight"]
    df = spark.createDataFrame(data, columns)

    df_result = clean_orders(df)

    assert df_result.count() == 1
    assert df_result.collect()[0]["order_id"] == 101


def test_add_currency_column(spark):
    df_orders = spark.createDataFrame(
        [(101, "FRANCE", 100.0), (102, "USA", 200.0), (103, "JAPAN", 10000.0), (104, "UNKNOWN", 50.0)],
        ["order_id", "customer_country", "sous_total"]
    )
    df_currency = spark.createDataFrame(
        [("FRANCE", "EUR"), ("USA", "USD"), ("JAPAN", "JPY")],
        ["country", "currency"]
    )
    df_rates = spark.createDataFrame(
        [({"USD": 1.0, "EUR": 0.92, "JPY": 150.0},)],
        ["rates"]
    )

    df_result = enrich_with_currency(df_orders, df_currency, df_rates)
    results = {row["order_id"]: (row["currency"], row["sous_total_local"]) for row in df_result.collect()}

    assert results[101] == ("EUR", 92.0)
    assert results[102] == ("USD", 200.0)
    assert results[103] == ("JPY", 1500000.0)
    assert results[104] == (None, None)