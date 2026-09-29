from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.countDistinct(F.col("ss.units")).alias("unique_stores"),
        F.max(F.col("ss.revenue")).alias("max_revenue"),
    )
    .orderBy("unique_stores", F.col("city").desc())
)
