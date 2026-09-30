from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(8, 11))
    .select("status", F.lit("payment").alias("origin"))
    .unionByName(df_shipments.select("status", F.lit("shipment").alias("origin")))
)
