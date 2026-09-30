from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.avg("units").alias("avg_units"))
    .orderBy(F.col("avg_units").desc())
)
