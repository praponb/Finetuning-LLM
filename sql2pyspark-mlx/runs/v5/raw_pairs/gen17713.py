from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("revenue") <= 3300)
    .withColumn("units_str", F.col("units").cast("string"))
)

result = (
    filtered
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.city").alias("city"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.units")), 2).alias("total_units"),
        F.avg(F.col("a.revenue")).alias("avg_revenue"),
    )
    .orderBy(F.col("city").desc_nulls_first())
)
