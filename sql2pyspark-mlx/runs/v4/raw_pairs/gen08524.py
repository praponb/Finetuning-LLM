from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "left")
    .filter(F.col("pbo.amount_range") > 25)
    .select(F.col("o.id"), F.col("o.order_date"), F.col("pbo.amount_range"))
    .orderBy("id")
)
