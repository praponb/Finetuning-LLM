from pyspark.sql import functions as F

with_dims = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.amount"), F.col("o.channel"), F.col("o.shipping_fee"))
)

result = (
    with_dims
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "channel",
        "shipping_fee",
        F.when(F.col("carrier").rlike("^(Kerry|UPS)$"), "GROUP_A").otherwise("EXTENDED").alias("carrier_zone"),
    )
    .orderBy(F.col("delivered_date").desc())
)
