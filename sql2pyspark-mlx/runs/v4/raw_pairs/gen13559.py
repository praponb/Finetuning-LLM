from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy(F.col("paid_at_date").desc_nulls_first(), "id")

cleaned = (
    df_payments
    .filter(F.col("status") != "SUCCESS")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.substring("method", 1, 4).alias("method_prefix"),
    )
)

valid = (
    cleaned
    .filter(F.col("order_id") == 15)
    .select(
        "*",
        F.to_date("paid_at").alias("paid_at_date"),
        F.hour("paid_at").alias("paid_at_hour"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

with_prev = valid.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

deduped = (
    sessions_payments
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.status").alias("order_status"), F.col("p.channel"))
)

cleaned_payments = (
    enriched
    .filter(F.col("method").isNotNull())
    .select("*", F.initcap(F.trim("method")).alias("method_title"))
)

result = (
    cleaned_payments
    .withColumn("session_id_tier", F.when(F.col("session_id") >= 8, "PREMIUM").otherwise("STANDARD"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1320, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["session_id_tier", "method", "amount_tier"], ["session_id_tier", "method"], []],
        "session_id_tier",
        "method",
        "amount_tier",
    )
    .agg(
        F.max("prev_paid_at").alias("last_seen"),
        F.percentile_approx("session_id", [0.25, 0.5, 0.75]).alias("session_id_quartiles"),
        F.max(F.when(F.col("method") == "paypal", F.col("session_id"))).alias("paypal_max_session_id"),
        F.min("amount").alias("min_amount"),
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("is_new_burst_per_failed"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("session_id_tier").desc())
)
