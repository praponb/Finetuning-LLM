from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2024-04-01")
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .pivot("status", ["RETURNED", "IN_TRANSIT", "DELIVERED", "LOST"])
    .agg(F.avg("cost"))
)
