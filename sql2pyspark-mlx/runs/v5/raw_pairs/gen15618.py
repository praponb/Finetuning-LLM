from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select("status", F.lit("account").alias("record_type"))
    .unionByName(df_shipments.select("status", F.lit("shipment").alias("record_type")))
    .orderBy("record_type")
)
