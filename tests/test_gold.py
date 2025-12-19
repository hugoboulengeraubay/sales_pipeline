import pytest
from pyspark.sql import SparkSession
from sales_pipeline.gold.aggregation import aggregate_gold


@pytest.fixture(scope="module")
def spark():
    # Récupère la session active Databricks
    spark = SparkSession.getActiveSession()
    return spark

@pytest.fixture(scope="module")
def gold_result(spark):
    # Nettoyage AVANT test
    spark.sql("DROP TABLE IF EXISTS CA_gold")
    spark.sql("DROP TABLE IF EXISTS CA_par_boutique_gold")
    spark.sql("DROP TABLE IF EXISTS classement_produit_nombre_gold")
    spark.sql("DROP TABLE IF EXISTS classement_produit_montant_gold")

    return aggregate_gold(spark)

def test_gold_tables_exist(gold_result):
    expected_keys = [
        "CA_gold",
        "CA_par_boutique",
        "classement_nombre",
        "classement_montant"
    ]
    for key in expected_keys:
        assert key in gold_result


def test_ca_columns(gold_result):
    for df in [gold_result["CA_gold"], gold_result["CA_par_boutique"]]:
        assert "Annee_Mois" in df.columns
        assert "CA_EUR" in df.columns
        assert df.filter("CA_EUR IS NULL").count() == 0


def test_classement_columns(gold_result):
    assert "Nom_Produit" in gold_result["classement_nombre"].columns
    assert "Total" in gold_result["classement_nombre"].columns
    assert "Nom_Produit" in gold_result["classement_montant"].columns
    assert "Vente_Total" in gold_result["classement_montant"].columns


def test_gold_values_positive(gold_result):
    checks = [
        (gold_result["CA_gold"], "CA_EUR"),
        (gold_result["CA_par_boutique"], "CA_EUR"),
        (gold_result["classement_nombre"], "Total"),
        (gold_result["classement_montant"], "Vente_Total"),
    ]

    for df, col in checks:
        assert df.filter(f"{col} < 0").count() == 0
