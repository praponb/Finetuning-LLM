from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("o.store_id").isin([1, 7, 21, 37]))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
    )
)
