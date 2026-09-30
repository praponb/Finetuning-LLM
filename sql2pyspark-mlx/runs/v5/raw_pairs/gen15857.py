from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["UPS", "FedEx", "Kerry"])
    .agg(F.avg("cost"))
)
