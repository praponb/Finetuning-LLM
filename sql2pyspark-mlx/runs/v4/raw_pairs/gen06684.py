from pyspark.sql import functions as F

result = (
    df_payments
    .select("amount", F.lit("payment").alias("source"))
    .union(df_shipments.select("cost", F.lit("shipment").alias("source")))
    .distinct()
)
