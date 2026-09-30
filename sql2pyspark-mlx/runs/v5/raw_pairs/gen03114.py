from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())

filtered = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        (F.col("amount") * F.col("discount")).alias("amount_times_discount"),
        F.add_months("order_date", 1).alias("order_date_plus_months"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") >= F.col("s.order_date")), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.status",
        "s.channel",
        "s.amount",
        "s.discount",
        "s.shipping_fee",
        "s.order_date_plus_months",
        F.col("c.carrier"),
    )
)

cleaned = (
    with_last
    .filter(F.col("carrier") == "FedEx")
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "order_date_plus_months",
        "carrier",
        (F.col("discount") - F.col("shipping_fee")).alias("discount_minus_shipping_fee"),
    )
)

eligible_orders = (
    cleaned
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") <= F.col("s.order_date_plus_months")), "left_semi")
)

result = (
    eligible_orders
    .select(
        "id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "order_date_plus_months",
        "carrier",
        "discount_minus_shipping_fee",
    )
    .orderBy("status")
)
