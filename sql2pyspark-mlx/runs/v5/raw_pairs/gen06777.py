from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost", F.lit("shipment").alias("origin"))
    .union(df_payments.select("amount", F.lit("payment").alias("origin")))
    .orderBy("origin")
)
