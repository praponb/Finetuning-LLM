from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("s.carrier"),
        F.col("s.delivered_date"),
        F.col("s.cost"),
    )
    .orderBy("order_date", F.col("cost").desc())
)
