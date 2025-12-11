from pyspark.sql import SparkSession, functions as F

def clean_gold(spark: SparkSession, catalog_prefix="hboulenger_databricks"):
    """
    Calcule les tables Gold :
    - CA par mois
    - CA par boutique
    - Classement des produits (montant total)
    - Classement des produits (nombre de ventes)
    """

    # ============================
    #   Charger la table Silver
    # ============================
    df_silver = spark.table(f"{catalog_prefix}.silver.ventes")

    # ============================
    #   Conversion des devises → EUR
    # ============================
    taux = {"USD": 0.86, "EUR": 1.0, "JPY": 0.0057}

    df_silver = df_silver.withColumn(
        "Montant_EUR",
        F.when(F.col("Devise") == "EUR", F.col("Montant_Total"))
         .when(F.col("Devise") == "DOL", F.col("Montant_Total") * taux["USD"])
         .when(F.col("Devise") == "JPY", F.col("Montant_Total") * taux["JPY"])
    )

    # ============================
    #   Extraire Année-Mois
    # ============================
    df_silver = df_silver.withColumn(
        "Annee_Mois",
        F.date_format(F.to_date("Date_Vente", "dd/MM/yyyy"), "yyyy-MM")
    )

    # ============================
    #   CALCULS GOLD
    # ============================

    # ---- CA total mensuel ----
    df_CA_gold = df_silver.groupBy("Annee_Mois").agg(
        F.round(F.sum("Montant_EUR"), 2).alias("CA_EUR")
    ).orderBy("Annee_Mois")

    # ---- CA par boutique ----
    df_CA_par_boutique = df_silver.groupBy("Nom_Boutique", "Annee_Mois").agg(
        F.round(F.sum("Montant_EUR"), 2).alias("CA_EUR")
    ).orderBy("Annee_Mois")

    # ---- Classement produits : nombre total ----
    df_classement_nombre = df_silver.groupBy("Nom_Produit").agg(
        F.sum(F.col("Quantité")).alias("Total")
    ).orderBy(F.col("Total").desc())

    # ---- Classement produits : montant total ----
    df_classement_montant = df_silver.groupBy("Nom_Produit").agg(
        F.round(F.sum("Montant_Total"), 2).alias("Vente_Total")
    ).orderBy(F.col("Vente_Total").desc())

    # ============================
    #   ÉCRITURE DES TABLES GOLD
    # ============================

    df_CA_gold.write.format("delta").mode("append").saveAsTable(
        f"{catalog_prefix}.gold.CA_gold"
    )

    df_CA_par_boutique.write.format("delta").mode("append").saveAsTable(
        f"{catalog_prefix}.gold.CA_par_boutique_gold"
    )

    df_classement_nombre.write.format("delta").mode("append").saveAsTable(
        f"{catalog_prefix}.gold.classement_produit_nombre_gold"
    )

    df_classement_montant.write.format("delta").mode("append").saveAsTable(
        f"{catalog_prefix}.gold.classement_produit_montant_gold"
    )

    print("Tables Gold écrites avec succès.")

    # Optionnel : retourner tous les DataFrames pour tests
    return {
        "CA_gold": df_CA_gold,
        "CA_par_boutique": df_CA_par_boutique,
        "classement_nombre": df_classement_nombre,
        "classement_montant": df_classement_montant
    }
