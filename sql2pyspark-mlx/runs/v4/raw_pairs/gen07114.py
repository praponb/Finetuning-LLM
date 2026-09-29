from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date", F.lit("subscription").alias("source"))
    .union(
        df_shipments
        .select("delivered_date", F.lit("shipment").alias("source"))
    )
    .distinct()
)
