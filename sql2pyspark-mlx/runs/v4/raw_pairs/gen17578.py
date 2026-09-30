from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("p.method"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("c.country"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
