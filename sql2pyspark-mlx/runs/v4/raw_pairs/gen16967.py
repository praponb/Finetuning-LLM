from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("status").isin(["RETURNED", "PENDING"]) | (F.col("shipping_fee") <= 28))
)

status_summary = (
    filtered_orders
    .groupBy("status", F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.count("*").alias("order_count"),
        F.sum(F.when(F.col("channel") == "store", 1).otherwise(0)).alias("store_count"),
    )
)

result = (
    status_summary
    .filter(F.col("order_count") < 6)
    .orderBy("order_month", F.col("order_count").desc(), "status")
    .limit(20)
)
