from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 4, 8, 9]))
    .groupBy("store_id")
    .agg(F.avg("units").alias("avg_units"))
    .orderBy(F.col("store_id").desc(), "avg_units")
)
