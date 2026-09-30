from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(1700, 2900) & F.col("store_id").isin([1, 4, 5]) & F.col("units").between(31, 35))
    .withColumn("revenue_whole", F.floor("revenue"))
)
