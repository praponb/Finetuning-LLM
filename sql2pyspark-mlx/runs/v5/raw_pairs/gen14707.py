from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.amount").between(1020, 1280))
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("s.sqft"),
        F.col("c.segment"),
    )
    .orderBy(F.col("discount").desc())
)
