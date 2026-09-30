from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(F.countDistinct("amount").alias("unique_methods"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.status"), F.col("t.unique_methods"))
    .orderBy(F.col("id").desc())
    .limit(1)
)
