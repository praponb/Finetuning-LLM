from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .select(
        "id",
        "shipping_fee",
        F.concat_ws("/", "channel", "status").alias("channel_status_key"),
    )
    .orderBy("channel_status_key", "id")
    .limit(10)
)
