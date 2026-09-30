from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("c.is_active") | F.col("s.store_name").isNotNull())
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("c.credit_limit"),
        F.col("c.age"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
)
