from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date", F.lit("subscription").alias("record_type"))
    .unionByName(
        df_orders
        .select(
        F.col("order_date").alias("start_date"),
        F.lit("order").alias("record_type"),
    )
    )
    .distinct()
)
