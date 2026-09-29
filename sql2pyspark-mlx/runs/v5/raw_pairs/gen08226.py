from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.region").alias("region"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("src.revenue")).alias("total_revenue"),
        F.sum(F.col("src.units")).alias("total_units"),
    )
)
