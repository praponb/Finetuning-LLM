from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isin([2, 11, 37, 40]))
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.amount"),
        F.col("o.discount"),
        F.col("o.status"),
        F.col("o.customer_id"),
    )
)
