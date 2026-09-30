from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount")
w_grp = Window.partitionBy("status").orderBy("amount")
w3 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("order_id", "status").orderBy("amount_percent_rank", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
w6 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w7 = Window.partitionBy("method").orderBy("paid_at", "id")
w8 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w10 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("order_id").orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("amount_cume_dist_avg_3_prior", "id").rowsBetween(-6, Window.currentRow)

metrics = (
    df_payments
    .select(
        "*",
        F.cume_dist().over(w).alias("amount_cume_dist"),
        F.percent_rank().over(w_grp).alias("amount_percent_rank"),
        F.avg("amount").over(w3).alias("amount_avg_2_prior"),
    )
)

enriched = (
    metrics
    .withColumn("amount_cume_dist_avg_3_prior", F.avg("amount_cume_dist").over(w4))
    .withColumn("amount_cume_dist_2", F.cume_dist().over(w5))
    .withColumn("amount_avg_2_prior_avg_90d", F.avg("amount_avg_2_prior").over(w6))
)

flagged = (
    enriched
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w7).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w7) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_no", F.sum("session_start_flag").over(w8))

base = (
    sessions_payments
    .filter((F.col("paid_at") > F.lit("2024-02-08 20:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("amount_cume_dist_int", F.col("amount_cume_dist").cast("int"))
    .withColumn("session_no_whole", F.ceil("session_no"))
)

valid = (
    base
    .filter(F.col("method") != "card")
    .select(
        "*",
        (F.col("amount_avg_2_prior_avg_90d") + F.col("session_no")).alias("amount_avg_2_prior_avg_90d_plus_session_no"),
    )
)

prev_events = valid.withColumn("prev_paid_at", F.lag("paid_at").over(w9))

flagged_2 = (
    prev_events
    .withColumn("session_start_flag_2", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = flagged_2.withColumn("island_id", F.sum("session_start_flag_2").over(w10))

with_windows = (
    islands
    .select(
        "*",
        F.sum("session_no_whole").over(w_recent).alias("cumulative_session_no_whole"),
        F.avg("amount_cume_dist_2").over(w_running).alias("amount_cume_dist_2_avg_6"),
    )
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
        F.when(F.col("island_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("island_id_tier"),
    )
    .agg(
        F.percentile_approx("island_id", 0.9).alias("island_id_p90"),
        F.min("paid_at").alias("started_at"),
        F.percentile_approx("amount_avg_2_prior_avg_90d_plus_session_no", [0.25, 0.5, 0.75]).alias("amount_avg_2_prior_avg_90d_plus_session_no_quartiles"),
        F.percentile_approx("session_start_flag", [0.25, 0.5, 0.75]).alias("session_start_flag_quartiles"),
    )
    .filter(F.col("island_id_p90") > 0)
    .orderBy(F.col("island_id_tier").desc())
)
