from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("o.channel") == "partner") | (F.col("c.age") != 38))
    .select(
        F.col("s.delivered_date"),
        F.col("s.order_id"),
        F.col("o.amount"),
        F.col("c.country"),
        F.least(F.col("o.shipping_fee"), F.col("o.discount")).alias("least_shipping_fee_discount"),
    )
    .orderBy(F.col("country").desc())
)
