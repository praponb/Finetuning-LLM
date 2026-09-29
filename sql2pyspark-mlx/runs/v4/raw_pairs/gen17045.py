from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.quarter("order_date").alias("order_quarter"))
    .agg(F.round(F.avg("discount"), 2).alias("avg_discount"))
    .orderBy(F.col("avg_discount").desc(), "status")
)
