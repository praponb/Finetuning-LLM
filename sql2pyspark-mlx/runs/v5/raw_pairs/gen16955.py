from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.min("units").alias("min_units"),
    )
    .orderBy("unique_products", F.col("store_id").desc())
    .limit(1)
)
