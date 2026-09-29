from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("c.country"),
        F.col("s.region"),
        F.col("s.opened_date"),
    )
)
