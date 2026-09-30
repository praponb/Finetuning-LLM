from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(~F.col("c.is_active") | ~F.col("p.status").isin(["REFUNDED", "SUCCESS", "FAILED"]))
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("o.channel"),
        F.col("c.segment"),
        F.col("c.name"),
    )
)
