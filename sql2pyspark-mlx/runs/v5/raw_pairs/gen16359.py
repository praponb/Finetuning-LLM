from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter(F.col("o.amount") < 1040)
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("o.amount"),
        F.col("o.customer_id"),
    )
)
