from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("units").alias("min_units"),
    )
    .filter(F.col("total_count") >= 3)
)
