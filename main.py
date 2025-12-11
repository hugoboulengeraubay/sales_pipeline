from sales_pipeline.utils.spark_session import get_spark_session
from sales_pipeline.bronze.ingestion import ingest_bronze
from sales_pipeline.silver.cleaning import clean_silver
from sales_pipeline.gold.aggregation import aggregate_gold

if __name__ == "__main__":
    spark = get_spark_session()

    print("=== BRONZE ===")
    ingest_bronze(spark)

    print("=== SILVER ===")
    clean_silver(spark)

    print("=== GOLD ===")
    aggregate_gold(spark)

    print("Pipeline exécuté avec succès !")
