from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.count("*").alias("row_count"),
        F.max("discount").alias("max_discount"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("t.row_count"),
        F.col("t.max_discount"),
    )
    .orderBy(F.col("row_count").desc(), F.col("max_discount").desc(), "id")
    .limit(1)
)
