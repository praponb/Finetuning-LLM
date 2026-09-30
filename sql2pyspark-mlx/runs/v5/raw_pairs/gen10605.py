from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("o.shipping_fee") <= 23)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("c.age"),
        F.col("c.credit_limit"),
        F.col("s.sqft"),
        F.col("s.region"),
    )
)
