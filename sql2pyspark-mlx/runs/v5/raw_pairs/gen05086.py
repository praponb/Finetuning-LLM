from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.min("revenue").alias("min_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(F.col("s.id"), F.col("s.city"), F.col("ss.min_revenue"))
    .orderBy(F.col("city").desc(), F.col("id").desc())
    .limit(5)
)
