from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4, 5]) & F.col("units").between(21, 26))
    .groupBy("product_id")
    .agg(F.countDistinct("units").alias("unique_stores"))
    .orderBy("product_id")
)
