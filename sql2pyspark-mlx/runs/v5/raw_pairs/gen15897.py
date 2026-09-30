from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(30, 39) & F.col("product_id").isin([3, 7, 8, 10]))
    .groupBy("store_id")
    .agg(F.countDistinct("units").alias("unique_stores"))
    .orderBy(F.col("unique_stores").desc(), "store_id")
    .limit(1)
)
