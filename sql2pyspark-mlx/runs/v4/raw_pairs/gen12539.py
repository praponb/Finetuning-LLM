from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("paid_at") >= F.to_timestamp(F.lit("2023-07-31 06:00:00")))
    .select("*", F.coalesce("method", F.lit("N/A")).alias("method_filled"))
)

with_gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_payments
    .groupBy(F.date_trunc("WEEK", "paid_at").alias("week_start"))
    .agg(
        F.sum(F.when(F.col("method") == "cash", F.col("new_group_flag"))).alias("cash_total_new_group_flag"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
        F.percentile_approx("new_group_flag", 0.5).alias("new_group_flag_p50"),
        F.percentile_approx("session_no", 0.5).alias("session_no_p50"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_rows"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("bank_transfer_rows").desc_nulls_first())
)
