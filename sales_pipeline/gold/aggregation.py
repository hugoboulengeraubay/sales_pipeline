import logging
from pyspark.sql import SparkSession, functions as F

logger = logging.getLogger(__name__)


def aggregate_gold(spark: SparkSession):
    """
    Calcule les tables Gold :
    - Chiffre d'affaires mensuel
    - Chiffre d'affaires par boutique
    - Classement des produits (montant total)
    - Classement des produits (nombre de ventes)
    """

    logger.info("Start Gold aggregation")

    # Chargement Silver
    df_silver = spark.table("default.ventes")

    # Conversion des devises vers EUR
    taux = {
        "EUR": 1.0,
        "DOL": 0.86,
        "JPY": 0.0057
    }

    df_silver = df_silver.withColumn(
        "Montant_EUR",
        F.when(F.col("Devise") == "EUR", F.col("Montant_Total"))
         .when(F.col("Devise") == "DOL", F.col("Montant_Total") * taux["DOL"])
         .when(F.col("Devise") == "JPY", F.col("Montant_Total") * taux["JPY"])
    )

    # Extraction Année-Mois
    df_silver = df_silver.withColumn(
        "Annee_Mois",
        F.date_format(
            F.to_date("Date_Vente", "dd/MM/yyyy"),
            "yyyy-MM"
        )
    )

    # CA total mensuel
    df_CA_gold = (
        df_silver
        .groupBy("Annee_Mois")
        .agg(F.round(F.sum("Montant_EUR"), 2).alias("CA_EUR"))
        .orderBy("Annee_Mois")
    )

    # CA par boutique
    df_CA_par_boutique = (
        df_silver
        .groupBy("Nom_Boutique", "Annee_Mois")
        .agg(F.round(F.sum("Montant_EUR"), 2).alias("CA_EUR"))
        .orderBy("Annee_Mois")
    )

    # Classement produits par quantité
    df_classement_nombre = (
        df_silver
        .groupBy("Nom_Produit")
        .agg(F.sum("Quantité").alias("Total"))
        .orderBy(F.col("Total").desc())
    )

    # Classement produits par montant
    df_classement_montant = (
        df_silver
        .groupBy("Nom_Produit")
        .agg(F.round(F.sum("Montant_EUR"), 2).alias("Vente_Total"))
        .orderBy(F.col("Vente_Total").desc())
    )

    # Écriture Gold
    df_CA_gold.write.format("delta").mode("append").saveAsTable("CA_gold")
    df_CA_par_boutique.write.format("delta").mode("append").saveAsTable("CA_par_boutique_gold")
    df_classement_nombre.write.format("delta").mode("append").saveAsTable("classement_produit_nombre_gold")
    df_classement_montant.write.format("delta").mode("append").saveAsTable("classement_produit_montant_gold")

    logger.info("Gold tables successfully written")

    return {
        "CA_gold": df_CA_gold,
        "CA_par_boutique": df_CA_par_boutique,
        "classement_nombre": df_classement_nombre,
        "classement_montant": df_classement_montant
    }



# Pour le test CI (à modifier)
def compute_gold_from_df(df_silver):
    """
    Calcul Gold à partir d'un DataFrame Silver (testable en CI)
    """
    taux = {
        "EUR": 1.0,
        "DOL": 0.86,
        "JPY": 0.0057
    }

    df = df_silver.withColumn(
        "Montant_EUR",
        F.when(F.col("Devise") == "EUR", F.col("Montant_Total"))
         .when(F.col("Devise") == "DOL", F.col("Montant_Total") * taux["DOL"])
         .when(F.col("Devise") == "JPY", F.col("Montant_Total") * taux["JPY"])
    )

    df = df.withColumn(
        "Annee_Mois",
        F.date_format(
            F.to_date("Date_Vente", "dd/MM/yyyy"),
            "yyyy-MM"
        )
    )

    df_CA_gold = (
        df.groupBy("Annee_Mois")
          .agg(F.round(F.sum("Montant_EUR"), 2).alias("CA_EUR"))
    )

    df_CA_par_boutique = (
        df.groupBy("Nom_Boutique", "Annee_Mois")
          .agg(F.round(F.sum("Montant_EUR"), 2).alias("CA_EUR"))
    )

    df_classement_nombre = (
        df.groupBy("Nom_Produit")
          .agg(F.sum("Quantité").alias("Total"))
    )

    df_classement_montant = (
        df.groupBy("Nom_Produit")
          .agg(F.round(F.sum("Montant_EUR"), 2).alias("Vente_Total"))
    )

    return {
        "CA_gold": df_CA_gold,
        "CA_par_boutique": df_CA_par_boutique,
        "classement_nombre": df_classement_nombre,
        "classement_montant": df_classement_montant
    }

