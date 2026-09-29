from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.name").like("%bob%") & (F.col("c.signup_date") < "2025-08-06"))
    .select(
        F.col("o.status"),
        F.col("o.shipping_fee"),
        F.col("c.signup_date"),
        F.col("c.city"),
    )
    .orderBy("status")
)
