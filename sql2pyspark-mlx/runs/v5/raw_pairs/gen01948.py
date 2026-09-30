from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNotNull())
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") >= 6900)
    .select("order_month", "total_amount")
)
