from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select("shipped_date", F.lit("shipment").alias("source"))
    .union(df_accounts.select("opened_date", F.lit("account").alias("source")))
    .orderBy("source")
)
