from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("cost") <= 27)
    .select("*", F.round("cost", 0).alias("cost_rounded"))
)

result = (
    cleaned
    .groupBy("status", "order_id")
    .pivot("carrier", ["FedEx", "Flash"])
    .agg(F.sum("cost_rounded"))
)
