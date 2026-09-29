from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("s.region"),
        F.col("c.segment"),
        F.col("c.city"),
    )
)
