from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.count("units").alias("units_count"))
    .orderBy(F.col("units_count").desc())
)
