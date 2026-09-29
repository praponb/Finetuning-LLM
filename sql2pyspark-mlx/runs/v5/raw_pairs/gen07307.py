from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "shipping_fee",
        "discount",
        F.lower("channel").alias("channel_lower"),
    )
)
