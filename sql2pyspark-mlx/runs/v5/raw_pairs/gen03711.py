from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("payment_method").orderBy(F.col("discount").desc_nulls_first())
w5 = Window.partitionBy("payment_method")

cleaned = (
    df_orders
    .filter((F.col("channel").eqNullSafe(F.lit("store"))) & (F.col("order_date") >= F.lit("2023-08-08").cast("date")))
    .withColumn("channel_title", F.initcap(F.trim("channel")))
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.status",
        "s.channel",
        "s.discount",
        "s.shipping_fee",
        "s.channel_title",
        F.col("c.method").alias("payment_method"),
        F.col("c.status").alias("payment_status"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status",
        "s.channel",
        "s.discount",
        "s.payment_method",
        "s.payment_status",
        F.col("c.paid_at").alias("payment_paid_at"),
        F.col("c.amount").alias("payment_amount"),
    )
)

with_latest = (
    with_last
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.payment_paid_at")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.status",
        "s.discount",
        "s.payment_method",
        "s.payment_paid_at",
        "s.payment_amount",
        F.col("c.status").alias("shipment_status"),
    )
)

cleaned_orders = (
    with_latest
    .filter(F.col("shipment_status").isNotNull() & (~F.col("shipment_status").eqNullSafe(F.lit("RETURNED"))))
    .select(
        "id",
        "status",
        "discount",
        "payment_method",
        F.when(F.col("shipment_status").isin(["RETURNED", "DELIVERED"]), "Group A").otherwise("Other").alias("shipment_status_group"),
        F.coalesce("payment_method", F.lit("UNKNOWN")).alias("payment_method_clean"),
    )
)

with_windows = (
    cleaned_orders
    .withColumn("discount_rank", F.rank().over(w4))
    .withColumn("payment_method_avg_discount", F.avg("discount").over(w5))
)

result = (
    with_windows
    .select(
        "id",
        "status",
        "discount",
        "payment_method",
        "shipment_status_group",
        "payment_method_clean",
        "discount_rank",
        "payment_method_avg_discount",
        F.when(F.col("status").rlike("^(COMPLETED|SHIPPED)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("status_zone"),
    )
)
