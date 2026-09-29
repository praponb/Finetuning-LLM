from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") >= "2023-09-01")
    .groupBy("store_id")
    .agg(F.sum("discount").alias("total_discount"))
    .orderBy(F.col("store_id").desc(), F.col("total_discount").desc())
)
