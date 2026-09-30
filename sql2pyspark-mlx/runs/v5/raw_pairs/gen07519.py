from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("discount", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-07-06"))) & F.col("store_id").isNotNull())
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.lower("status").alias("status_lower"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "status",
        "channel",
        "discount",
        "status_lower",
        "channel_filled",
        F.lead("shipping_fee", 1).over(w).alias("next_shipping_fee"),
        F.ntile(3).over(w2).alias("discount_bucket"),
    )
)
