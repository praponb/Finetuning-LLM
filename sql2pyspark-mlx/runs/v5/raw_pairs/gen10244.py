from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("method") == "paypal").alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .filter(F.col("o.amount").between(1120, 1350))
    .select(F.col("o.id"), F.col("o.customer_id"))
)
