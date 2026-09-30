from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") <= 57)
    .select(F.col("shipped_date").alias("event_date"))
    .intersect(
        df_subscriptions
        .filter(F.col("status") == "ACTIVE")
        .select(F.col("start_date").alias("event_date"))
    )
)
