from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("discount") > 0.33)
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
        F.upper("channel").alias("channel_upper"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

result = (
    base
    .select("status", "channel")
    .intersect(
        df_orders
        .filter(F.col("amount").between(890, 1510))
        .select("status", "channel")
    )
)
