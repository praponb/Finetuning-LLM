from pyspark.sql import functions as F

result = (
    df_payments
    .select(F.lit("payment").alias("record_type"), "status")
    .unionByName(df_shipments.select(F.lit("shipment").alias("record_type"), "status"))
)
