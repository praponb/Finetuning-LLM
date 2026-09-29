from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "customer_id").orderBy(F.col("order_date").desc(), "id")
w2 = Window.orderBy(F.col("shipping_fee").desc())

filtered = (
    df_orders
    .filter(F.col("channel").isNull())
    .withColumn("amount_with_shipping", (F.col("amount") + F.col("shipping_fee")))
    .withColumn("shipping_fee_times_discount", (F.col("shipping_fee") * F.col("discount")))
)

cleaned = (
    filtered
    .filter(F.col("channel").isNotNull() & F.col("customer_id").isNotNull())
    .withColumn("clean_channel", F.trim("channel"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("N/A")))
)

with_windows = (
    cleaned
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("shipping_fee_percent_rank", F.percent_rank().over(w2))
)

kept = (
    with_windows
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") <= F.col("s.order_date")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "customer_id",
        "amount",
        "shipping_fee",
        "shipping_fee_percent_rank",
        "amount_with_shipping",
    )
    .unpivot(
        ["id", "customer_id"],
        ["amount", "shipping_fee", "shipping_fee_percent_rank", "amount_with_shipping"],
        "metric",
        "val",
    )
    .filter(F.col("val").isNotNull())
)
