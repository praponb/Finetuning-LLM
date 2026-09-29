from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .pivot("carrier", ["FedEx", "Flash"])
    .agg(F.avg("cost"))
    .select("order_id", F.col("FedEx").alias("fedex"), F.col("Flash").alias("flash"))
)
