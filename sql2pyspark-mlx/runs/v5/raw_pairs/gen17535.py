from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.customer_id") == 21)
    .select(
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("c.country"),
    )
    .orderBy("status")
)
