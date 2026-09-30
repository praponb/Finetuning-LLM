from pyspark.sql import functions as F

store_totals = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.avg("revenue").alias("avg_revenue"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("t.unique_products"),
        F.col("t.avg_revenue"),
    )
    .orderBy("city")
)
