from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "right")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.store_id"),
        F.col("c.is_active"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
    .orderBy("id", F.col("store_id").desc())
)
