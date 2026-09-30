from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.lit("shipment").alias("kind"), "status")
    .unionByName(df_subscriptions.select(F.lit("subscription").alias("kind"), "status"))
    .orderBy("status")
)
