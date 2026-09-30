from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(F.min("units").alias("min_units"))
    .orderBy("min_units")
)
