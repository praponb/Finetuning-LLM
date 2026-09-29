from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .withColumn("amount_rounded", F.round("amount", 0))
    .withColumn("shipping_fee_whole", F.ceil("shipping_fee"))
)

result = (
    filtered
    .select("store_id", "id")
    .exceptAll(
        df_orders
        .filter(F.col("discount") >= 0.25)
        .select("store_id", "id")
    )
)
