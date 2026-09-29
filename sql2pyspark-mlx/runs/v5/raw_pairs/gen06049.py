from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee") < 34)
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("o.status"),
    )
    .orderBy("status", "id")
    .limit(20)
)
