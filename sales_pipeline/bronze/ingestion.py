from pyspark.sql import SparkSession

def ingest_bronze(spark: SparkSession, data_root: str, catalog_prefix: str = "hboulenger_databricks.bronze", ok=True):
    """
    Ingestion des CSV depuis DBFS vers les tables Bronze.
    Archive les fichiers traités, sinon déplace en 'failed'.
    """

    # Lister les sous-dossiers
    subfolders = [f.path for f in dbutils.fs.ls(data_root) if f.isDir()]

    for folder in subfolders:
        table_name = folder.rstrip("/").split("/")[-1]
        print(f"--- Traitement dossier : {table_name} ---")
        print(f"  -> {folder}")

        # Fichiers CSV
        csv_files = [
            f.path for f in dbutils.fs.ls(folder)
            if f.name.endswith(".csv") and f.name not in ("archives", "failed")
        ]

        for file_path in csv_files:
            print(f"  Lecture du fichier : {file_path}")

            df = spark.read.csv(file_path, header=True, inferSchema=True)
            file_dir = "/".join(file_path.split("/")[:-1])

            if ok:
                archive_folder = f"{file_dir}/archives"
                dbutils.fs.mkdirs(archive_folder)
                archive_path = f"{archive_folder}/{file_path.split('/')[-1]}"

                df.write.format("delta").mode("append").saveAsTable(
                    f"{catalog_prefix}.{table_name}"
                )

                dbutils.fs.mv(file_path, archive_path)
                print(f"    ✔ Archivé dans : {archive_path}")

            else:
                failed_folder = f"{file_dir}/failed"
                dbutils.fs.mkdirs(failed_folder)
                failed_path = f"{failed_folder}/{file_path.split('/')[-1]}"

                dbutils.fs.mv(file_path, failed_path)
                print(f"    ❌ Erreur → déplacé dans FAILED : {failed_path}")

    print("Ingestion Bronze terminée.")
