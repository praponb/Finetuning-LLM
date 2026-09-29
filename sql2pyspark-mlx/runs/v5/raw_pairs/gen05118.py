from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .pivot("carrier", ["FedEx", "Kerry", "DHL", "Flash"])
    .agg(F.sum("cost"))
    .orderBy("order_id")
)
