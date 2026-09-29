from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2025-01-01")
    .groupBy("status")
    .pivot("carrier", ["Kerry", "FedEx", "UPS", "Flash"])
    .agg(F.sum("cost"))
    .orderBy("status")
)
