from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2024-12-07")
    .groupBy("status")
    .pivot("carrier", ["FedEx", "DHL", "UPS"])
    .agg(F.sum("cost"))
    .orderBy("status")
)
