from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("shipped_date") < "2023-06-01")
    .withColumn("days_delivered_date_to_shipped_date", F.datediff("shipped_date", "delivered_date"))
)

summary = (
    recent_shipments
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
    )
)

result = summary.orderBy(F.col("carrier_values").desc())
