from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("units").alias("max_units"))
    .filter(F.col("max_units") > 18)
    .orderBy(F.col("max_units").desc(), F.col("product_id").desc())
    .limit(10)
)
