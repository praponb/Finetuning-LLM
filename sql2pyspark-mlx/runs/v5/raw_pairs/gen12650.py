from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .filter(F.col("o.shipping_fee").between(12, 28))
    .select(
        F.col("o.amount"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
    )
    .orderBy("amount")
)
