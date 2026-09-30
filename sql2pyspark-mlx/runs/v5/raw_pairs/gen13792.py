from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date").between("2024-01-24", "2025-07-13"))
    .groupBy(F.col("s.region"))
    .agg(
        F.round(F.stddev(F.col("ss.revenue")), 2).alias("stddev_revenue"),
        F.max(F.col("ss.units")).alias("max_units"),
    )
    .filter(F.col("stddev_revenue") > 70)
    .orderBy("region")
)
