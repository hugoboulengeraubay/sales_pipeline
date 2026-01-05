import logging

from sales_pipeline.utils.spark_session import get_spark_session
from sales_pipeline.bronze.ingestion import ingest_bronze
from sales_pipeline.silver.cleaning import clean_silver
from sales_pipeline.gold.aggregation import aggregate_gold
from sales_pipeline.utils.logging_config import setup_logging


def main(dbutils=None):
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting Sales Pipeline")

    spark = get_spark_session("SalesPipeline")

    bronze = ingest_bronze(
        spark=spark,
        data_root="dbfs:/Volumes/workspace/default/hboulenger_databricks/bronze/raw_data/",
        dbutils=dbutils
    )

    clean_silver(spark)
    aggregate_gold(spark)

    logger.info("Sales Pipeline finished successfully")



if __name__ == "__main__":
    main()
