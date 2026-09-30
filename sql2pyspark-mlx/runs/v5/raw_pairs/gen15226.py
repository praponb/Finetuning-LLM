from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull() & F.col("store_id").isin([3, 4]))
    .withColumn("revenue_whole", F.floor("revenue"))
)
