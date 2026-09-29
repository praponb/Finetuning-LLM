from pyspark.sql import functions as F

recent_shipments = df_shipments.filter((F.col("order_id") == 3) & (F.col("status") == "IN_TRANSIT"))

carrier_summary = (
    recent_shipments
    .groupBy("carrier", "status")
    .agg(
        F.count("cost").alias("cost_count"),
        F.min("delivered_date").alias("min_delivered_date"),
    )
)

result = carrier_summary.filter(F.col("cost_count") < 9)
