from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .filter(F.col("o.amount") >= 610)
    .groupBy(F.col("o.channel"))
    .agg(
        F.max(F.col("s.delivered_date")).alias("last_delivered_date"),
        F.count(F.when(F.col("s.carrier") == "DHL", 1)).alias("dhl_count"),
    )
    .orderBy("last_delivered_date")
)
