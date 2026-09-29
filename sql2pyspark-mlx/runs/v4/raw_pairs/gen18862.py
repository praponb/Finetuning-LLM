from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "FedEx", "UPS"]))
    .groupBy("status")
    .pivot("carrier", ["FedEx", "UPS", "Kerry"])
    .agg(F.avg("cost"))
)
