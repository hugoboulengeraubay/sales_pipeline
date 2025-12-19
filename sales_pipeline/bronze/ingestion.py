import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def ingest_bronze(
    spark: SparkSession,
    data_root: str,
    dbutils
):
    """
    Ingestion des fichiers CSV depuis DBFS vers les tables Bronze (Delta).

    - Chaque sous-dossier devient une table
    - Les fichiers traités sont archivés
    - En cas d'erreur, les fichiers sont déplacés dans 'failed'
    """

    logger.info("Start Bronze ingestion")

    subfolders = [f.path for f in dbutils.fs.ls(data_root) if f.isDir()]

    for folder in subfolders:
        table_name = folder.rstrip("/").split("/")[-1]

        logger.info(f"Processing folder: {folder}")
        logger.info(f"Target table: {table_name}")

        csv_files = [
            f.path for f in dbutils.fs.ls(folder)
            if f.name.endswith(".csv") and f.name not in ("archives", "failed")
        ]

        for file_path in csv_files:
            logger.info(f"Reading file: {file_path}")
            file_dir = "/".join(file_path.split("/")[:-1])

            try:
                df = spark.read.csv(file_path, header=True, inferSchema=True)

                df.write.format("delta") \
                    .mode("append") \
                    .saveAsTable(table_name)

                archive_folder = f"{file_dir}/archives"
                dbutils.fs.mkdirs(archive_folder)
                archive_path = f"{archive_folder}/{file_path.split('/')[-1]}"

                dbutils.fs.mv(file_path, archive_path)
                logger.info(f"File archived: {archive_path}")

            except Exception as e:
                failed_folder = f"{file_dir}/failed"
                dbutils.fs.mkdirs(failed_folder)
                failed_path = f"{failed_folder}/{file_path.split('/')[-1]}"

                dbutils.fs.mv(file_path, failed_path)
                logger.error(f"Error ingesting file {file_path}")
                logger.error(str(e))

    
    logger.info("Bronze ingestion completed")