from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(F.count(F.col("ss.revenue")).alias("revenue_count"))
    .orderBy("city")
)
