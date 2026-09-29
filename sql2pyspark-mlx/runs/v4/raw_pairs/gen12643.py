from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("channel").isin(["web", "partner"])).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.carrier").isin(["Kerry", "DHL"]))
    .select(F.col("s.id"), F.col("s.delivered_date"))
)
