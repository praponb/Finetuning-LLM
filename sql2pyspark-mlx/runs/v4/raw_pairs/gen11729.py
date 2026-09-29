from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5]))
    .select("*", F.col("quantity").cast("string").alias("quantity_num"))
)

result = (
    valid
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("activity_month"))
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("total_quantity").desc())
)
