from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.carrier").isin(["DHL", "UPS", "FedEx"]))
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.delivered_date"),
        F.col("o.channel"),
        F.col("o.discount"),
        F.col("o.order_date"),
    )
)
