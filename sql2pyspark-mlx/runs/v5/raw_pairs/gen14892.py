from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipping_fee").asc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w_grp = Window.orderBy("shipping_fee", "id")
w_ordered = Window.orderBy("payment_amount", "id")

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-06-08").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "status",
        "shipping_fee",
        (F.col("discount") / F.nullif("shipping_fee", F.lit(0))).alias("discount_per_shipping_fee"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "status", "shipping_fee", "discount_per_shipping_fee")
)

filtered = (
    latest
    .filter(F.col("shipping_fee").between(10, 17))
    .withColumn("status_group", F.when(F.col("status").isin(["PENDING", "RETURNED"]), "Tier 1").otherwise("Other"))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.shipping_fee",
        "s.discount_per_shipping_fee",
        F.col("c.amount").alias("payment_amount"),
    )
)

enriched = (
    with_latest
    .withColumn("next_payment_amount", F.lead("payment_amount").over(w_grp))
    .withColumn("prev_shipping_fee", F.lag("shipping_fee").over(w_grp))
    .withColumn("seq_no", F.row_number().over(w_grp))
    .withColumn("seq_no_2", F.row_number().over(w_ordered))
)

result = (
    enriched
    .select(
        "id",
        "discount_per_shipping_fee",
        "payment_amount",
        "next_payment_amount",
        "seq_no",
        "seq_no_2",
    )
    .orderBy(F.col("seq_no_2").desc())
)
