from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.shipping_fee").isNotNull())
    .select("s.*", F.col("o.channel"), F.col("o.discount"), F.col("o.order_date"))
)
