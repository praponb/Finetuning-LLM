from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .pivot("carrier", ["UPS", "FedEx"])
    .agg(F.count("cost"))
    .orderBy("status")
)
