from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("amount") < 1330))
    .withColumn("amount_with_shipping", (F.col("amount") - F.col("shipping_fee")))
    .withColumn("amount_times_discount", (F.col("amount") * F.col("discount")))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("NONE")))
)

filtered = (
    valid
    .filter((F.col("order_date") >= F.lit("2023-09-18").cast("date")) & F.col("channel").isNotNull())
    .select("*", F.weekofyear("order_date").alias("order_date_weekofyear"))
)

valid_orders = (
    filtered
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        (F.col("discount") * F.col("shipping_fee")).alias("discount_times_shipping_fee"),
        F.when(F.col("amount") >= 1690, "High").when(F.col("amount") >= 440, "Medium").otherwise("Low").alias("amount_tier"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel_2"),
    )
)

with_last = (
    valid_orders
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.status",
        "s.channel",
        "s.shipping_fee",
        "s.amount_with_shipping",
        "s.amount_times_discount",
        "s.discount_times_shipping_fee",
        "s.amount_tier",
        "s.resolved_channel_2",
        F.col("c.status").alias("payment_status"),
        F.col("c.paid_at").alias("payment_paid_at"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.payment_paid_at")), "left_anti")
)

result = (
    eligible
    .groupBy("store_id")
    .pivot("payment_status", ["REFUNDED", "SUCCESS", "FAILED"])
    .agg(F.max("discount_times_shipping_fee"))
    .select(
        "store_id",
        F.col("REFUNDED").alias("refunded"),
        F.col("SUCCESS").alias("success"),
        F.col("FAILED").alias("failed"),
    )
)
