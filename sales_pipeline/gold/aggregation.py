from pyspark.sql import SparkSession, functions as F

def aggregate_silver(spark: SparkSession, catalog_prefix="hboulenger_databricks"):
    """
    Nettoyage, harmonisation et construction de la table Silver 'ventes'.
    """

    # --- Chargement Bronze ---
    df_catalogue = spark.table(f"{catalog_prefix}.bronze.catalogue_produits")
    df_paris = spark.table(f"{catalog_prefix}.bronze.boutique_paris")
    df_tokyo = spark.table(f"{catalog_prefix}.bronze.boutique_tokyo")
    df_ny = spark.table(f"{catalog_prefix}.bronze.boutique_new_york")

    # ============================
    #   HARMONISATION NEW YORK
    # ============================
    df_ny = (
        df_ny
        .withColumnRenamed("ID_Sale","ID_Vente")
        .withColumnRenamed("Sale_Date","Date_Vente")
        .withColumnRenamed("Product_Name","Nom_Produit")
        .withColumnRenamed("Category","Catégorie")
        .withColumnRenamed("Unit_Price","Prix_Unitaire")
        .withColumnRenamed("Quantity", "Quantité")
        .withColumnRenamed("Total_Amount", "Montant_Total")
        .withColumn("Nom_Boutique", F.lit("boutique_new_york"))
        .withColumn("Ville", F.lit("NewYork"))
        .withColumn("Pays", F.lit("USA"))
        .withColumn("Devise", F.lit("DOL"))
    )

    # ============================
    #   HARMONISATION TOKYO
    # ============================
    df_tokyo = (
        df_tokyo
        .withColumnRenamed("ID_Sale","ID_Vente")
        .withColumnRenamed("Sale_Date","Date_Vente")
        .withColumnRenamed("Product_Name","Nom_Produit")
        .withColumnRenamed("Category","Catégorie")
        .withColumnRenamed("Unit_Price","Prix_Unitaire")
        .withColumnRenamed("Quantity", "Quantité")
        .withColumnRenamed("Total_Amount", "Montant_Total")
        .withColumn("Nom_Boutique", F.lit("boutique_tokyo"))
        .withColumn("Ville", F.lit("Tokyo"))
        .withColumn("Pays", F.lit("Japon"))
        .withColumn("Devise", F.lit("JPY"))
    )

    # ============================
    #   PARIS
    # ============================
    df_paris = (
        df_paris
        .withColumn("Nom_Boutique", F.lit("boutique_paris"))
        .withColumn("Ville", F.lit("Paris"))
        .withColumn("Pays", F.lit("France"))
        .withColumn("Devise", F.lit("EUR"))
    )

    # ============================
    #   GESTION DES IDs
    # ============================

    try:
        df_silver_existing = spark.table(f"{catalog_prefix}.silver.ventes")
        max_id = df_silver_existing.agg(F.max("ID_Vente")).collect()[0][0] or 0
    except:
        max_id = 0

    def add_ids(df, start_id):
        return df.withColumn(
            "ID_Vente", 
            F.monotonically_increasing_id() + start_id + 1
        )

    df_paris = add_ids(df_paris, max_id)
    max_id += df_paris.count()

    df_tokyo = add_ids(df_tokyo, max_id)
    max_id += df_tokyo.count()

    df_ny = add_ids(df_ny, max_id)
    max_id += df_ny.count()

    # ============================
    #   FORMATTER DATES
    # ============================
    df_ny = df_ny.withColumn(
        "Date_Vente",
        F.date_format(F.to_date("Date_Vente", "yyyy-MM-dd"), "dd/MM/yyyy")
    )

    df_tokyo = df_tokyo.withColumn(
        "Date_Vente",
        F.date_format(F.to_date("Date_Vente", "yyyy-dd-MM"), "dd/MM/yyyy")
    )

    df_paris = df_paris.withColumn(
        "Date_Vente",
        F.date_format(F.to_date("Date_Vente", "yyyy-MM-dd"), "dd/MM/yyyy")
    )

    # ============================
    #   JOINTURES AVEC CATALOGUE
    # ============================

    # Nom Produit
    for df in [df_ny, df_tokyo]:
        df = df.join(
            df_catalogue.select("Nom_Produit_Anglais", "Nom_Produit_Francais"),
            df["Nom_Produit"] == F.col("Nom_Produit_Anglais"),
            "left"
        )
        df = df.drop("Nom_Produit", "Nom_Produit_Anglais") \
               .withColumnRenamed("Nom_Produit_Francais", "Nom_Produit")

    # Catégories
    for df in [df_ny, df_tokyo]:
        df = df.join(
            df_catalogue.select("Catégorie_Anglais", "Catégorie_Francais").dropDuplicates(),
            df["Catégorie"] == F.col("Catégorie_Anglais"),
            "left"
        )
        df = df.drop("Catégorie", "Catégorie_Anglais") \
               .withColumnRenamed("Catégorie_Francais", "Catégorie")

    # ============================
    #   UNION FINALE SILVER
    # ============================
    df_silver = df_paris.unionByName(df_tokyo).unionByName(df_ny)

    # ============================
    #   ECRITURE SILVER
    # ============================
    df_silver.write.format("delta").mode("append").saveAsTable(
        f"{catalog_prefix}.silver.ventes"
    )

    print("Table Silver 'ventes' mise à jour.")
