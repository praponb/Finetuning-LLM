from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-03-04"))) & (F.col("channel") == "store"))
    .select(
        "id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "shipping_fee",
        F.floor("shipping_fee").alias("shipping_fee_whole"),
        F.when(F.col("amount") >= 1560, "High").when(F.col("amount") >= 640, "Medium").otherwise("Low").alias("amount_level"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.channel",
        "s.amount",
        "s.shipping_fee",
        F.col("c.method").alias("payment_method"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.status") == "RETURNED"), "left_anti")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.amount", "s.shipping_fee", F.col("c.paid_at"))
)

with_last = (
    with_latest
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.paid_at").alias("payment_paid_at"))
)

result = with_last.select("id", "amount", "shipping_fee", "paid_at", "payment_paid_at")
