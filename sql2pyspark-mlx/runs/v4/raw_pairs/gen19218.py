from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select(
        "id",
        "channel",
        F.concat_ws("/", "status", "channel").alias("status_channel_key"),
    )
)
