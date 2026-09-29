from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") >= "2025-04-03")
    .select(F.lit("order").alias("record_type"), "status")
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id").isNotNull())
        .select(F.lit("subscription").alias("record_type"), "status")
    )
)
