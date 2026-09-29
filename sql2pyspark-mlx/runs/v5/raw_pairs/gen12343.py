from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") > 1600)
    .groupBy("store_id")
    .agg(
        F.min("units").alias("min_units"),
        F.count("units").alias("units_count"),
    )
    .filter(F.col("units_count") >= 5)
    .select("store_id", "min_units", "units_count")
)
