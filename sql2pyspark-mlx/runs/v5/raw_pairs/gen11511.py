from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.col("discount").isNotNull())
    .groupBy("store_id")
    .agg(F.max("discount").alias("max_discount"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(F.col("s.opened_date"), F.col("ss.max_discount"))
)
