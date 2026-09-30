from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .select(
        F.col("p.order_id"),
        F.col("p.status"),
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
)
