from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("order_date").alias("latest_order_date"),
    )
    .filter(F.col("avg_amount") >= 52)
)
