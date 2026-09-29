from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("s.shipped_date") < "2025-04-05")
    .select(
        F.col("s.status"),
        F.col("s.cost"),
        F.col("o.customer_id"),
        F.col("c.signup_date"),
        (F.col("o.discount") - F.col("o.shipping_fee")).alias("discount_minus_shipping_fee"),
    )
)
