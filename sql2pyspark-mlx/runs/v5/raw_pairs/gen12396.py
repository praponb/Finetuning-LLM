from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["region", "city"], ["region"], []],
        F.col("d.region").alias("region"),
        F.col("d.city").alias("city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.revenue")).alias("max_revenue"),
        F.max(F.col("src.units")).alias("max_units"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
