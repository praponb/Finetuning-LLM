from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_first(), "id")

valid = (
    df_payments
    .filter(F.col("paid_at") > F.to_timestamp(F.lit("2023-08-03 23:00:00")))
    .select(
        "*",
        F.date_trunc("MONTH", "paid_at").alias("paid_at_period"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .filter(F.col("d.customer_id").isin([10, 27, 31]))
    .select(
        "s.id",
        "s.order_id",
        "s.amount",
        "s.status",
        "s.paid_at_period",
        "s.method_clean",
        F.col("d.order_date"),
        F.col("d.channel"),
    )
)

base = (
    enriched
    .filter(F.hour("paid_at_period").between(12, 17))
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "paid_at_period",
        "method_clean",
        "channel",
        F.concat_ws("-", "channel", "status").alias("channel_status_key"),
        F.col("paid_at_period").cast("date").alias("paid_at_period_day"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.amount",
        "src.status",
        "src.channel",
        "src.paid_at_period_day",
        F.col("o.channel").alias("order_channel"),
        F.col("o.discount"),
        F.col("o.status").alias("order_status"),
    )
)

latest = (
    joined
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "amount", "status", "order_status")
)

latest_payments = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_status")
)

cleaned = (
    latest_payments
    .filter(F.col("order_status").eqNullSafe(F.lit("COMPLETED")))
    .select(
        "*",
        F.when(F.col("order_status") == "RETURNED", 1).otherwise(0).alias("is_returned"),
        F.coalesce("order_status", F.lit("DIRECT")).alias("order_status_filled"),
    )
)

result = (
    cleaned
    .select("id", "order_status")
    .orderBy("id")
    .limit(10)
)
