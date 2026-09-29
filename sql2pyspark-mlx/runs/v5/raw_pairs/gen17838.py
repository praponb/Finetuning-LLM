from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter(F.col("channel") == "partner")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.col("discount").cast("int").alias("discount_int"),
        F.substring("channel", 1, 4).alias("channel_prefix"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

result = (
    filtered
    .select("store_id", "status")
    .intersectAll(
        df_orders
        .filter(F.col("amount").between(630, 1530))
        .select("store_id", "status")
    )
)
