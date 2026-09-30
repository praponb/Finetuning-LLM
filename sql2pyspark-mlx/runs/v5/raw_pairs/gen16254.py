from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .pivot("status", ["LOST", "IN_TRANSIT"])
    .agg(F.max("cost"))
    .select(
        "order_id",
        F.col("LOST").alias("lost"),
        F.col("IN_TRANSIT").alias("in_transit"),
    )
)
