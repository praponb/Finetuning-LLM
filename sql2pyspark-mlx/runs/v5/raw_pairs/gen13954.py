from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull())
    .select(F.lit("account").alias("kind"), "status")
    .unionByName(df_shipments.select(F.lit("shipment").alias("kind"), "status"))
    .distinct()
)
