from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.orderBy(F.col("amount").desc_nulls_last())
w3 = Window.partitionBy("order_id").orderBy(F.col("amount_cume_dist").desc_nulls_first(), "id")
w4 = Window.partitionBy("method", "status").orderBy(F.col("amount_cume_dist").asc_nulls_first())
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w6 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "card"]))
    .select(
        "*",
        F.lead("amount").over(w).alias("next_amount"),
        F.lag("amount").over(w).alias("prev_amount"),
        F.row_number().over(w).alias("event_seq"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    ranked
    .withColumn("rank_in_group_2", F.rank().over(w4))
    .filter(F.col("rank_in_group_2") == 1)
)

with_gaps = (
    latest
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w5).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w5) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "next_amount",
        "prev_amount",
        "event_seq",
        "amount_cume_dist",
        "rank_in_group",
        F.sum("session_start_flag").over(w6).alias("island_id"),
    )
)

kept_keys = (
    sessionized
    .select("id")
    .intersect(
        df_payments
        .filter(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]))
        .select("id")
    )
)

reconciled = (
    sessionized
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.amount",
        "src.paid_at",
        "src.status",
        "src.next_amount",
        "src.prev_amount",
        "src.amount_cume_dist",
        "src.rank_in_group",
        F.col("d.discount"),
        F.col("d.shipping_fee"),
    )
)

result = (
    with_dims
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .groupingSets(
        [["activity_month", "order_id"], ["activity_month"], []],
        "activity_month",
        "order_id",
    )
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", F.col("prev_amount")).otherwise(0)).alias("failed_prev_amount"),
        F.count("*").alias("num_rows"),
    )
)
