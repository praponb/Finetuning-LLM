from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") >= 1)
    .orderBy(F.col("distinct_store_ids").desc_nulls_last())
)
