from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.col("method") == "cash")
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .filter(F.col("ot.paypal_count") >= 35)
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.store_id"),
        F.col("ot.paypal_count"),
    )
    .orderBy("amount", "id")
    .limit(5)
)
