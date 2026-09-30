from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .select(
        F.col("payments.id"),
        F.col("payments.status"),
        F.col("payments.amount"),
        F.col("orders.order_date"),
    )
    .orderBy("order_date", "status", "id")
    .limit(3)
)
