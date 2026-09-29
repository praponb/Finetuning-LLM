from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter(F.col("shipped_date") > "2025-03-04")
    .withColumn("days_since_shipped_date", F.datediff(F.current_date(), "shipped_date"))
)

delivered_month_summary = (
    filtered_shipments
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .agg(F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"))
)

result = (
    delivered_month_summary
    .select("delivered_month", "lost_count")
    .orderBy("delivered_month")
)
