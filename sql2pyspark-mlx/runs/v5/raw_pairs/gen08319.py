from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("units") > 13)
    .select("*", F.ceil("revenue").alias("revenue_whole"))
)

result = (
    filtered
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.region").alias("region"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.revenue")).alias("avg_revenue"),
    )
)
