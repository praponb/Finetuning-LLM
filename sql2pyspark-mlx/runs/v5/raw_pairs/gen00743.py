from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "right")
    .filter(F.col("s.region").isin(["East", "West"]) & F.col("o.customer_id").isin([7, 15]))
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("c.country"),
        F.col("c.signup_date"),
        F.col("s.store_name"),
        F.col("s.city"),
    )
)
