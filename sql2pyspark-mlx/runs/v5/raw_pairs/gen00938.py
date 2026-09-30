from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.shipping_fee").between(24, 30))
    .select(
        F.col("p.amount"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("c.age"),
        F.col("c.country"),
        F.floor(F.col("o.amount")).alias("amount_whole"),
    )
)
