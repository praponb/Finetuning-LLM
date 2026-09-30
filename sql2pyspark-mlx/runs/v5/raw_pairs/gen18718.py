from pyspark.sql import functions as F

dim_totals = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .rollup(F.col("s.city").alias("city"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.units")).alias("max_units"),
        F.min(F.col("e.revenue")).alias("min_revenue"),
    )
)

result = dim_totals.select("city", "cnt", "max_units", "min_revenue")
