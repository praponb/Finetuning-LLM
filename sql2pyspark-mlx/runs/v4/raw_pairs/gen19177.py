from pyspark.sql import functions as F

valid_shipments = df_shipments.filter(F.col("shipped_date") > "2025-01-10")

summary = (
    valid_shipments
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
        F.max("delivered_date").alias("latest_delivered_date"),
    )
)

result = (
    summary
    .filter(F.col("lost_count") >= 4)
    .select("status", "lost_count", "latest_delivered_date")
    .orderBy(F.col("lost_count").desc())
)
