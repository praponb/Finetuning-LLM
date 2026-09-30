from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date", F.lit("shipment").alias("record_type"))
    .union(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("start_date", F.lit("subscription").alias("record_type"))
    )
    .distinct()
    .orderBy("delivered_date")
)
