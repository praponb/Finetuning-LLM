from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("s.opened_date"),
        F.col("c.age"),
        F.coalesce(F.col("c.email"), F.lit("Unknown")).alias("email_filled"),
    )
    .orderBy(F.col("age").desc())
)
