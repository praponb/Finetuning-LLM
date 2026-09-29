from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.lit("subscription").alias("source"), "end_date")
    .union(df_shipments.select(F.lit("shipment").alias("source"), "shipped_date"))
)
