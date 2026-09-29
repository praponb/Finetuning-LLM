from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("o.store_id").isin([1, 10, 28]))
    .select(
        F.col("c.is_active"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
