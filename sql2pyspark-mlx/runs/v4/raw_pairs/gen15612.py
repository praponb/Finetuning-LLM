from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at_period", "id")
w2 = Window.partitionBy("method").orderBy("paid_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2024-01-18 22:00:00").cast("timestamp"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.date_trunc("WEEK", "paid_at").alias("paid_at_period"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

flagged = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("paid_at_period").over(w).isNull() | (F.col("paid_at_period") > F.lag("paid_at_period").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .withColumn("week_start", F.date_trunc("WEEK", "paid_at"))
    .groupingSets([["week_start", "method"], ["week_start"]], "week_start", "method")
    .agg(
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("status") == "SUCCESS", 1)), F.lit(0)), 2).alias("session_start_flag_per_success"),
        F.count_if(F.col("status") == "REFUNDED").alias("refunded_events"),
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("method") == "bank_transfer", 1)), F.lit(0)), 2).alias("session_start_flag_per_bank_transfer"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_rows"),
        F.max(F.when(F.col("method") == "promptpay", F.col("session_start_flag"))).alias("promptpay_max_session_start_flag"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("session_start_flag_per_success") > 0)
)
