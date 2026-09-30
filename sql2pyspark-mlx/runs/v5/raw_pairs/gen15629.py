from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.country") == "DE")
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("o.discount"),
        F.col("c.segment"),
        F.col("c.signup_date"),
    )
)
