from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy("is_refunded", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("status").orderBy("paid_at", "id")

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-07-11").cast("date")) & (~F.col("method").eqNullSafe(F.lit("card"))))
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
        F.concat_ws("-", "status", "method").alias("status_method_key"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

enriched = (
    base
    .withColumn("prev_is_refunded", F.lag("is_refunded").over(w_recent))
    .withColumn("prev_amount", F.lag("amount").over(w_recent))
    .withColumn("next_amount", F.lead("amount").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent))
)

metrics = (
    enriched
    .filter(F.to_date("paid_at") >= "2025-02-01")
    .select(
        "*",
        F.avg("amount").over(w2).alias("running_avg_amount"),
        F.min("amount").over(w2).alias("running_min_amount"),
        F.max("amount").over(w2).alias("running_max_amount"),
        F.count("amount").over(w2).alias("running_count_amount"),
        F.stddev_samp("seq_no").over(w3).alias("status_stddev_samp_seq_no"),
    )
)

gaps = (
    metrics
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w4) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    gaps
    .select(
        "id",
        "paid_at",
        "status",
        "is_refunded",
        "resolved_method",
        "prev_is_refunded",
        "prev_amount",
        "next_amount",
        "seq_no",
        "running_avg_amount",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

base_payments = (
    sessions_payments
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-10-24")))
    .select("*", F.floor("session_no").alias("session_no_whole"))
)

result = (
    base_payments
    .withColumn("session_no_whole_tier", F.when(F.col("session_no_whole") >= 88, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .rollup("session_no_whole_tier", "activity_month")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_amount", 0.9).alias("prev_amount_p90"),
        F.sum("prev_is_refunded").alias("total_prev_is_refunded"),
        F.grouping("session_no_whole_tier").alias("grouping_level"),
    )
)
