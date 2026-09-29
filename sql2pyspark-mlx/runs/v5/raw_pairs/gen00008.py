from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

with_prev = islands.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps_2 = (
    with_prev
    .withColumn("is_new_burst_2", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands_payments = gaps_2.withColumn("session_no", F.sum("is_new_burst_2").over(w2))

matched_keys = (
    islands_payments
    .select("order_id", "status")
    .subtract(
        df_payments
        .filter(F.col("amount").between(430, 1330))
        .select("order_id", "status")
    )
)

reconciled = (
    islands_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.shipping_fee"))
)

enriched_payments = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "inner")
    .select("src.*", F.col("o.order_date"), F.col("o.amount").alias("order_amount"))
)

summary = (
    enriched_payments
    .cube(
        F.when(F.col("amount") >= 1710, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.when(F.col("is_new_burst") >= 1, "LARGE").otherwise("SMALL").alias("is_new_burst_tier"),
        "method",
    )
    .agg(
        F.percentile_approx("is_new_burst_2", 0.9).alias("is_new_burst_2_p90"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.min("is_new_burst").alias("min_is_new_burst"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_rows"),
        F.max("prev_paid_at").alias("ended_at"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
)

result = (
    summary
    .select(
        "amount_tier",
        "is_new_burst_tier",
        "is_new_burst_2_p90",
        "duration_min",
        "min_is_new_burst",
        "ended_at",
    )
    .orderBy(F.col("duration_min").desc())
)
