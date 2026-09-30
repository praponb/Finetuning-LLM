from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "FedEx", "Flash"]))
    .groupBy("order_id")
    .agg(F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.dhl_count") >= 4)
    .select(F.col("o.discount"), F.col("o.order_date"), F.col("O.dhl_count"))
)
