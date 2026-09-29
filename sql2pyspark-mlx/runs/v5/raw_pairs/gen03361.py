from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("c.is_active") & (F.col("s.opened_date") > "2025-04-01"))
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("c.signup_date"),
        F.col("c.email"),
        F.col("s.sqft"),
        F.col("s.region"),
    )
)
