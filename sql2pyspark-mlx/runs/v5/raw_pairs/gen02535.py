from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(
        F.col("monthly_fee").alias("value"),
        F.lit("subscription").alias("record_type"),
    )
    .unionByName(
        df_orders
        .select(F.col("amount").alias("value"), F.lit("order").alias("record_type"))
    )
    .orderBy(F.col("value").desc())
)
