from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .pivot("status", ["SHIPPED", "CANCELLED", "RETURNED"])
    .agg(F.count("amount"))
    .orderBy("order_month")
)
