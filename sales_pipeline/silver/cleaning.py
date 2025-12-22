import logging
from pyspark.sql import SparkSession, functions as F

logger = logging.getLogger(__name__)


def add_metadata_columns(df, boutique, ville, pays, devise):
    return (
        df.withColumn("Nom_Boutique", F.lit(boutique))
          .withColumn("Ville", F.lit(ville))
          .withColumn("Pays", F.lit(pays))
          .withColumn("Devise", F.lit(devise))
    )


def add_ids(df, start_id: int):
    return df.withColumn(
        "ID_Vente",
        F.monotonically_increasing_id() + start_id + 1
    )


def harmonize_dates(df, input_format):
    return df.withColumn(
        "Date_Vente",
        F.date_format(F.to_date("Date_Vente", input_format), "dd/MM/yyyy")
    )


def translate_products(df, df_catalogue):
    df = df.join(
        df_catalogue.select("Nom_Produit_Anglais", "Nom_Produit_Francais"),
        df["Nom_Produit"] == F.col("Nom_Produit_Anglais"),
        "left"
    )
    return (
        df.drop("Nom_Produit", "Nom_Produit_Anglais")
          .withColumnRenamed("Nom_Produit_Francais", "Nom_Produit")
    )


def translate_categories(df, df_catalogue):
    df = df.join(
        df_catalogue.select(
            "Catégorie_Anglais", "Catégorie_Francais"
        ).dropDuplicates(),
        df["Catégorie"] == F.col("Catégorie_Anglais"),
        "left"
    )
    return (
        df.drop("Catégorie", "Catégorie_Anglais")
          .withColumnRenamed("Catégorie_Francais", "Catégorie")
    )


def clean_silver(spark: SparkSession):
    """
    Nettoyage, harmonisation et construction de la table Silver 'ventes'.
    """

    logger.info("Start Silver cleaning")

    # Chargement Bronze
    df_catalogue = spark.table("default.catalogue_produits")
    df_paris = spark.table("default.boutique_paris")
    df_tokyo = spark.table("default.boutique_tokyo")
    df_ny = spark.table("default.boutique_newyork")

    # Harmonisation New York
    df_ny = (
        df_ny
        .withColumnRenamed("ID_Sale", "ID_Vente")
        .withColumnRenamed("Sale_Date", "Date_Vente")
        .withColumnRenamed("Product_Name", "Nom_Produit")
        .withColumnRenamed("Category", "Catégorie")
        .withColumnRenamed("Unit_Price", "Prix_Unitaire")
        .withColumnRenamed("Quantity", "Quantité")
        .withColumnRenamed("Total_Amount", "Montant_Total")
    )
    df_ny = add_metadata_columns(
        df_ny, "boutique_new_york", "NewYork", "USA", "DOL"
    )

    # Harmonisation Tokyo
    df_tokyo = (
        df_tokyo
        .withColumnRenamed("ID_Sale", "ID_Vente")
        .withColumnRenamed("Sale_Date", "Date_Vente")
        .withColumnRenamed("Product_Name", "Nom_Produit")
        .withColumnRenamed("Category", "Catégorie")
        .withColumnRenamed("Unit_Price", "Prix_Unitaire")
        .withColumnRenamed("Quantity", "Quantité")
        .withColumnRenamed("Total_Amount", "Montant_Total")
    )
    df_tokyo = add_metadata_columns(
        df_tokyo, "boutique_tokyo", "Tokyo", "Japon", "JPY"
    )

    # Paris
    df_paris = add_metadata_columns(
        df_paris, "boutique_paris", "Paris", "France", "EUR"
    )

    # Gestion des IDs
    try:
        df_existing = spark.table("ventes")
        max_id = df_existing.agg(F.max("ID_Vente")).collect()[0][0] or 0
    except Exception:
        max_id = 0

    df_paris = add_ids(df_paris, max_id)
    max_id += df_paris.count()

    df_tokyo = add_ids(df_tokyo, max_id)
    max_id += df_tokyo.count()

    df_ny = add_ids(df_ny, max_id)

    # Harmonisation dates
    df_paris = harmonize_dates(df_paris, "yyyy-MM-dd")
    df_tokyo = harmonize_dates(df_tokyo, "yyyy-dd-MM")
    df_ny = harmonize_dates(df_ny, "yyyy-MM-dd")

    # Traductions
    df_tokyo = translate_products(df_tokyo, df_catalogue)
    df_ny = translate_products(df_ny, df_catalogue)

    df_tokyo = translate_categories(df_tokyo, df_catalogue)
    df_ny = translate_categories(df_ny, df_catalogue)

    # Union finale
    df_silver = df_paris.unionByName(df_tokyo).unionByName(df_ny)

    # Écriture Silver
    df_silver.write.format("delta").mode("append").saveAsTable("ventes")

    logger.info("Silver table 'ventes' updated")
