from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.min("units").alias("min_units"))
    .orderBy("store_id", "min_units")
    .limit(3)
)
