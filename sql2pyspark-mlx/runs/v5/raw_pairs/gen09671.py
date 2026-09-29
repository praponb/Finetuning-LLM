from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        (F.col("discount") + F.col("shipping_fee")).alias("discount_plus_shipping_fee"),
    )
)

result = (
    valid
    .select("channel", "id")
    .subtract(
        df_orders
        .filter(F.col("shipping_fee").between(35, 36))
        .select("channel", "id")
    )
)
