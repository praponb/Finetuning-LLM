from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("o.customer_id") == 33) & (F.col("s.opened_date") >= "2023-08-19"))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.status"),
        F.col("c.is_active"),
        F.col("s.city"),
        F.col("s.sqft"),
    )
)
