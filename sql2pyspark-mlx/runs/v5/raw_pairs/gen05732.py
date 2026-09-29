from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("c.credit_limit") <= 8600)
    .select(
        F.col("s.order_id"),
        F.col("s.carrier"),
        F.col("o.shipping_fee"),
        F.col("c.credit_limit"),
        F.col("c.age"),
        F.least(F.col("o.shipping_fee"), F.col("o.discount")).alias("least_shipping_fee_discount"),
    )
)
