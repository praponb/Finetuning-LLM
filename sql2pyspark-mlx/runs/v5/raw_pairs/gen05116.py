from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "status").orderBy(F.col("amount").asc_nulls_last(), "id")
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("method")

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

ranked = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

prev_events = ranked.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

enriched = (
    sessionized
    .select(
        "*",
        F.max("amount").over(w4).alias("order_id_max_amount"),
        F.stddev_samp("island_id").over(w5).alias("method_stddev_samp_island_id"),
    )
)

summary = (
    enriched
    .rollup("order_id", "method")
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.grouping("order_id").alias("grouping_id"),
    )
)

result = (
    summary
    .select(
        "method",
        F.when(F.col("method").rlike("^(card|bank_transfer)$"), "PRIORITY").when(F.col("method").rlike("^(paypal|cash)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("method_zone"),
    )
    .orderBy("method")
)
