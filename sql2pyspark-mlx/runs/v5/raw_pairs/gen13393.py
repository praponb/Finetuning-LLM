from pyspark.sql import functions as F

valid_orders = df_orders.filter(F.col("customer_id") == 37)

order_month_summary = (
    valid_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(F.countDistinct("amount").alias("unique_channels"))
)

result = (
    order_month_summary
    .filter(F.col("unique_channels") < 9)
    .select("order_month", "unique_channels")
    .orderBy("order_month")
)
