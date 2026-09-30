from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2024-02-09").cast("date")) & F.col("amount").between(870, 1500))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        F.floor("amount").alias("amount_whole"),
        (F.col("shipping_fee") - F.col("discount")).alias("shipping_fee_minus_discount"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

base_orders = (
    base
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "status",
        "channel",
        "amount_whole",
        "shipping_fee_minus_discount",
        "channel_clean",
        F.floor("shipping_fee_minus_discount").alias("shipping_fee_minus_discount_whole"),
        (F.col("shipping_fee_minus_discount") - F.col("shipping_fee")).alias("shipping_fee_minus_discount_minus_shipping_fee"),
    )
)

cleaned = (
    base_orders
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "status",
        "channel",
        "amount_whole",
        "shipping_fee_minus_discount",
        "channel_clean",
        "shipping_fee_minus_discount_whole",
        (F.col("shipping_fee_minus_discount_whole") / F.nullif("shipping_fee_minus_discount_minus_shipping_fee", F.lit(0))).alias("shipping_fee_minus_discount_whole_per_shipping_fee_minus_discount_minus_shipping_fee"),
        F.coalesce("shipping_fee_minus_discount_whole", F.lit(0)).alias("shipping_fee_minus_discount_whole_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "status",
        "amount_whole",
        "shipping_fee_minus_discount",
        "shipping_fee_minus_discount_whole",
        "shipping_fee_minus_discount_whole_per_shipping_fee_minus_discount_minus_shipping_fee",
        "shipping_fee_minus_discount_whole_filled",
    )
)
