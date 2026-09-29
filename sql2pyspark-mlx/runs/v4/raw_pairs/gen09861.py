from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "left")
    .filter(F.col("pbo.row_count") >= 9)
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("pbo.row_count"),
    )
)
