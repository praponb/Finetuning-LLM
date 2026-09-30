from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("amount_tier").orderBy(F.col("shell_amount").desc())

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.date_format("txn_time", "MMM yyyy").alias("txn_time_label"),
        F.trim("txn_type").alias("clean_txn_type"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

flagged = (
    valid
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("amount") >= 4200, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.date_trunc("week", "txn_time").alias("week_start"),
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "Shell", F.col("amount")).otherwise(0)).alias("shell_amount"),
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
    )
    .filter(F.col("avg_session_start_flag") > 0)
    .withColumn("shell_amount_percent_rank", F.percent_rank().over(w3))
)
