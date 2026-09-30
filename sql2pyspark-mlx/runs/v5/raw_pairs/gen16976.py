from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .select(
        F.col("p.method"),
        F.col("p.status"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
    )
    .orderBy("method", "order_date")
)
