from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 20) & F.col("customer_id").isNull())
    .select(
        "channel",
        "status",
        "customer_id",
        F.floor("discount").alias("discount_whole"),
        F.trim("channel").alias("clean_channel"),
    )
)
