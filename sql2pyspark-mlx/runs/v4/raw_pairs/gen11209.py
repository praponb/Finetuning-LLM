from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("amount") <= 890)
    .groupBy("order_id")
    .agg(
        F.countDistinct("amount").alias("unique_statuss"),
        F.countDistinct("amount").alias("unique_orders"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("P"), F.col("o.id") == F.col("P.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("P.unique_statuss"),
        F.col("P.unique_orders"),
    )
    .orderBy("order_date")
)
