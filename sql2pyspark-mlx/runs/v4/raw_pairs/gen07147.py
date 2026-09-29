from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("NONE")))
)

with_gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(
        "account_id",
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.when(F.col("amount") >= 3100, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles"),
        F.min("session_id").alias("min_session_id"),
        F.min("txn_time").alias("first_seen"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_rows"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("amount_int")).otherwise(0)).alias("credit_amount_int"),
        F.count_if(F.col("merchant") == "Shell").alias("shell_events"),
    )
    .filter(F.col("shell_events") > 1)
)
