from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("dim"), F.col("e.store_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.region").alias("region"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("e.units")).alias("avg_units"),
        F.min(F.col("e.revenue")).alias("min_revenue"),
    )
)
