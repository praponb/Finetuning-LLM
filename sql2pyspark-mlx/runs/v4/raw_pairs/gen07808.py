from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.store_id") == 18)
    .select(
        F.col("o.status"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("c.email"),
        F.col("c.country"),
    )
)
