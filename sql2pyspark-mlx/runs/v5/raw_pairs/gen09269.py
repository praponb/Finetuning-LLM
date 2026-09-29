from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("status", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("method").orderBy(F.col("next_is_new_session").asc_nulls_last(), "id")
w7 = Window.partitionBy("method").orderBy("is_new_session_avg_90d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("method").orderBy(F.col("paid_at").desc(), "id")
w9 = Window.partitionBy("event_seq_tier").orderBy(F.col("cnt").desc())

valid = (
    df_payments
    .filter(F.col("paid_at") > F.to_timestamp(F.lit("2023-06-14 11:00:00")))
    .select("*", F.initcap(F.trim("status")).alias("status_title"))
)

with_gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

with_windows = (
    sessionized
    .filter(F.col("status").isin(["SUCCESS", "FAILED"]))
    .select(
        "id",
        "method",
        "paid_at",
        "island_id",
        F.lead("is_new_session", 1, 0).over(w3).alias("next_is_new_session"),
        F.last("method", ignorenulls=True).over(w4).alias("last_known_method"),
        F.avg("is_new_session").over(w_ordered).alias("is_new_session_avg_90d"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.sum("is_new_session_avg_90d").over(w7).alias("cumulative_is_new_session_avg_90d"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("event_seq") >= 8, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
        "island_id",
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.sum(F.when(F.col("method") == "paypal", F.col("cumulative_is_new_session_avg_90d"))).alias("paypal_total_cumulative_is_new_session_avg_90d"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("island_id"))).alias("promptpay_total_island_id"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("method") == "paypal", 1)), F.lit(0)), 2).alias("event_seq_per_paypal"),
        F.sum(F.when(F.col("method") == "card", F.col("island_id")).otherwise(0)).alias("card_island_id"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("span_minutes") >= 0)
    .withColumn("cnt_percent_rank", F.percent_rank().over(w9))
    .orderBy("event_seq_tier")
)
