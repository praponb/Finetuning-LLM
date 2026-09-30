from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "right")
    .filter(F.col("c.segment").isNull() & (F.col("c.segment") == "SMB"))
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.channel"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("c.credit_limit"),
    )
)
