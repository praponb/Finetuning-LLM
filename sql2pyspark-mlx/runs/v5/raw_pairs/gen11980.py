from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 32)
    .select(F.lit("order").alias("source"), "status")
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee").isNotNull())
        .select(F.lit("subscription").alias("source"), "status")
    )
)
