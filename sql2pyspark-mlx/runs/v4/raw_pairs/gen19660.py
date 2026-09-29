from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("order_id").isNotNull())

carrier_summary = (
    recent_shipments
    .groupBy("carrier", "status")
    .agg(
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
    )
)

result = (
    carrier_summary
    .orderBy("lost_cost", F.col("in_transit_count").desc(), "carrier", "status")
    .limit(1)
)
