from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("shipping_fee").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

eligible = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.quantity") > 3), "left_anti")
)

eligible_orders = (
    eligible
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.unit_price") < 190), "left_semi")
)

deduped = (
    eligible_orders
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

filtered = (
    deduped
    .filter(F.col("channel").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("store"))))
    .withColumn("channel_group", F.when(F.col("channel").isin(["web", "store"]), "Core").otherwise("Other"))
    .withColumn("discount_minus_shipping_fee", (F.col("discount") - F.col("shipping_fee")))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") <= F.col("s.order_date")), "left_anti")
)

eligible_orders_2 = (
    not_flagged
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.delivered_date") <= F.col("s.order_date")), "left_anti")
)

with_last = (
    eligible_orders_2
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.channel",
        "s.discount",
        "s.shipping_fee",
        "s.rn",
        "s.channel_group",
        F.col("c.status").alias("payment_status"),
        F.col("c.amount").alias("payment_amount"),
    )
)

result = (
    with_last
    .groupBy("store_id")
    .pivot("channel", ["web", "mobile"])
    .agg(F.max("payment_amount"))
)
