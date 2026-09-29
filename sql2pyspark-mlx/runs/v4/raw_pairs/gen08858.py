from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee", F.lit("subscription").alias("source"))
    .union(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("cost", F.lit("shipment").alias("source"))
    )
)
