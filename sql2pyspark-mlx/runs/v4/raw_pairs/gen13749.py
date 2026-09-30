from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(F.sum("discount").alias("total_discount"))
    .filter(F.col("total_discount") > 1.7)
)
