from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter(F.col("cost") <= 74)
    .withColumn("days_delivered_date_to_shipped_date", F.datediff("shipped_date", "delivered_date"))
)

summary = (
    filtered_shipments
    .groupBy(
        "status",
        F.when(F.col("days_delivered_date_to_shipped_date") >= 82, "high").when(F.col("days_delivered_date_to_shipped_date") >= 11, "mid").otherwise("low").alias("days_delivered_date_to_shipped_date_bucket"),
    )
    .agg(F.max("delivered_date").alias("last_delivered_date"))
)

result = (
    summary
    .select(
        "status",
        "days_delivered_date_to_shipped_date_bucket",
        "last_delivered_date",
    )
    .orderBy(
        "last_delivered_date",
        "status",
        "days_delivered_date_to_shipped_date_bucket",
    )
    .limit(10)
)
