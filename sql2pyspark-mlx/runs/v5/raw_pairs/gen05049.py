from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.avg(F.col("ss.revenue")).alias("avg_revenue"),
        F.count(F.col("ss.units")).alias("units_count"),
    )
    .orderBy(F.col("units_count").desc())
)
