from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost", F.lit("shipment").alias("record_type"))
    .union(df_payments.select("amount", F.lit("payment").alias("record_type")))
)
