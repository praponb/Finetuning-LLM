from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "right")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("orders.channel"))
    .agg(F.sum(F.col("payments.amount")).alias("total_amount"))
    .filter(F.col("total_amount") >= 7200)
    .orderBy(F.col("total_amount").desc())
)
