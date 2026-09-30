from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.orderBy(F.col("order_amount").desc_nulls_last())
w_recent = Window.partitionBy("method", "channel")
w_hist = Window.orderBy(F.col("order_amount").desc_nulls_first())

base = (
    df_payments
    .filter((F.col("paid_at") > F.to_timestamp(F.lit("2023-07-25 02:00:00"))) & (F.col("method").eqNullSafe(F.lit("card"))))
    .withColumn("paid_at_hour", F.hour("paid_at"))
)

joined = (
    base
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.shipping_fee") >= 23)
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.amount",
        "a.paid_at",
        "a.status",
        "a.paid_at_hour",
        F.col("d.discount"),
    )
)

enriched = (
    joined
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.channel"), F.col("p.amount").alias("order_amount"))
)

kept_keys = (
    enriched
    .select("id", "order_id")
    .intersect(
        df_payments
        .filter(F.col("status") != "FAILED")
        .select("id", "order_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("status", "method")
    .subtract(
        df_payments
        .filter(F.col("paid_at") >= "2024-07-01")
        .select("status", "method")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

enriched_payments = (
    reconciled
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.order_id",
        "a.method",
        "a.amount",
        "a.paid_at_hour",
        "a.discount",
        "a.channel",
        "a.order_amount",
        F.col("p.shipping_fee"),
    )
)

enriched_payments_2 = (
    enriched_payments
    .select(
        "discount",
        "channel",
        "order_amount",
        "shipping_fee",
        F.max("order_amount").over(w).alias("order_id_max_order_amount"),
        F.cume_dist().over(w2).alias("order_amount_cume_dist"),
        F.count("amount").over(w_recent).alias("method_count_amount"),
    )
)

result = (
    enriched_payments_2
    .select(
        "order_amount",
        "method_count_amount",
        F.rank().over(w_hist).alias("order_amount_rank"),
    )
)
