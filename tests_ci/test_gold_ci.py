import pytest
from pyspark.sql import SparkSession
from sales_pipeline.gold.aggregation import compute_gold_from_df


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("ci-tests")
        .getOrCreate()
    )


@pytest.fixture
def silver_df(spark):
    data = [
        ("01/01/2024", "Produit A", "boutique_paris", "EUR", 2, 100.0),
        ("02/01/2024", "Produit A", "boutique_paris", "EUR", 1, 50.0),
        ("01/01/2024", "Produit B", "boutique_tokyo", "JPY", 3, 10000.0),
        ("05/01/2024", "Produit C", "boutique_ny", "DOL", 1, 200.0),
    ]

    return spark.createDataFrame(
        data,
        [
            "Date_Vente",
            "Nom_Produit",
            "Nom_Boutique",
            "Devise",
            "Quantité",
            "Montant_Total"
        ]
    )

def test_gold_keys(silver_df):
    result = compute_gold_from_df(silver_df)
    assert set(result.keys()) == {
        "CA_gold",
        "CA_par_boutique",
        "classement_nombre",
        "classement_montant"
    }

def test_ca_columns(silver_df):
    result = compute_gold_from_df(silver_df)

    for df in [result["CA_gold"], result["CA_par_boutique"]]:
        assert "Annee_Mois" in df.columns
        assert "CA_EUR" in df.columns

def test_values_positive(silver_df):
    result = compute_gold_from_df(silver_df)

    checks = [
        ("CA_gold", "CA_EUR"),
        ("CA_par_boutique", "CA_EUR"),
        ("classement_nombre", "Total"),
        ("classement_montant", "Vente_Total"),
    ]

    for key, col in checks:
        assert result[key].filter(f"{col} < 0").count() == 0

def test_currency_conversion_jpy(silver_df):
    result = compute_gold_from_df(silver_df)

    df = result["classement_montant"]
    jpy_row = df.filter("Nom_Produit = 'Produit B'").collect()[0]

    # 10000 * 0.0057
    assert round(jpy_row["Vente_Total"], 2) == round(10000 * 0.0057, 2)
