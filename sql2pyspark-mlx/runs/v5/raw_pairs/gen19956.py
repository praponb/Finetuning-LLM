from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        F.lit("shipment").alias("record_type"),
        F.col("delivered_date").alias("event_date"),
    )
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date") > "2024-11-01")
        .select(
        F.lit("subscription").alias("record_type"),
        F.col("start_date").alias("event_date"),
    )
    )
    .distinct()
    .orderBy("record_type")
)
