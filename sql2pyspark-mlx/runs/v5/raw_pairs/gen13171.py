from pyspark.sql import functions as F

store_stats = (
    df_orders
    .filter(F.col("status") != "PENDING")
    .groupBy("store_id")
    .agg(
        F.countDistinct("amount").alias("unique_stores"),
        F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0)).alias("returned_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.col("t.unique_stores"),
        F.col("t.returned_count"),
    )
)
