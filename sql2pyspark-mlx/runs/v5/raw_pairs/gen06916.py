from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.status"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
        F.col("o.discount"),
    )
    .orderBy(F.col("amount").desc())
)
