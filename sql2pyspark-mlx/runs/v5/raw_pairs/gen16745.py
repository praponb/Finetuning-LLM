from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.status") == "CANCELLED")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("c.age"),
        F.round(F.col("o.amount"), 2).alias("amount_rounded"),
    )
)
