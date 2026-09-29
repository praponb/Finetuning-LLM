from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.shipping_fee") <= 35)
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.method"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("c.name"),
        F.col("c.country"),
    )
    .orderBy(F.col("discount").desc())
)
