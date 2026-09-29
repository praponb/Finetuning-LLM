from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.col("amount").between(500, 1030))
    .groupBy("order_id")
    .agg(F.count("*").alias("num_payments"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.num_payments") > 3)
    .select(F.col("o.id"), F.col("o.discount"), F.col("t.num_payments"))
)
