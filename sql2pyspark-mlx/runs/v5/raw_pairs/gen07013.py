from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-08-07").cast("date"))
    .withColumn("units_rounded", F.round("units", 2))
)

result = (
    filtered
    .alias("src")
    .join(df_stores.alias("dim"), F.col("src.store_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.region").alias("region"), F.col("dim.city").alias("city"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("src.revenue")).alias("total_revenue"),
        F.sum(F.col("src.units_rounded")).alias("total_units_rounded"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("region").desc())
)
