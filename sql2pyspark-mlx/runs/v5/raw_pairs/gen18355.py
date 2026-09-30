from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["IN_TRANSIT", "DELIVERED"])
    .agg(F.avg("cost").alias("amount"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "carrier",
        F.col("IN_TRANSIT_amount").alias("in_transit_amount"),
        F.col("IN_TRANSIT_item_count").alias("in_transit_item_count"),
        F.col("DELIVERED_amount").alias("delivered_amount"),
        F.col("DELIVERED_item_count").alias("delivered_item_count"),
    )
)
