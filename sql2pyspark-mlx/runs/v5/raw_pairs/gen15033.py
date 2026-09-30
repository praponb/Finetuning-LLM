from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    gaps
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_payments
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "paid_at"))
    .cube("activity_month", "quarter_start")
    .agg(
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles_2"),
        F.count_if(F.col("method") == "promptpay").alias("promptpay_events"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
        F.max(F.when(F.col("status") == "SUCCESS", F.col("burst_id"))).alias("success_max_burst_id"),
        F.grouping("activity_month").alias("grouping_id"),
    )
    .filter(F.col("promptpay_events") >= 1)
    .orderBy(F.col("amount_p90").desc_nulls_first())
)
