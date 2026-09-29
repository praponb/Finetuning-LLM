from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("end_date").alias("event_date"))
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select(F.col("delivered_date").alias("event_date"))
    )
    .distinct()
    .orderBy("event_date")
)
