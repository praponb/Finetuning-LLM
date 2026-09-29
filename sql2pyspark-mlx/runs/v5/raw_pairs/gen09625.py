from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .rollup(F.col("d.city").alias("city"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("e.units")), 2).alias("avg_units"),
        F.max(F.col("e.revenue")).alias("max_revenue"),
    )
    .orderBy("city")
)
