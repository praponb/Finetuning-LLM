from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("amount") < 640).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(~F.col("s.carrier").isin(["DHL", "FedEx"]))
    .select(F.col("s.id"), F.col("s.shipped_date"), F.col("s.order_id"))
    .orderBy("shipped_date")
)
