from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["RETURNED", "DELIVERED"])
    .agg(F.avg("cost"))
    .orderBy("carrier")
)
