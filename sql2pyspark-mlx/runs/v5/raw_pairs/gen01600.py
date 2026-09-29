from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("order_date") <= "2024-07-25")
    .withColumn("order_date_label", F.date_format("order_date", "MMM yyyy"))
)

agg = (
    filtered_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
    )
)

result = (
    agg
    .filter(F.col("avg_shipping_fee") >= 35)
    .orderBy("order_month")
)
