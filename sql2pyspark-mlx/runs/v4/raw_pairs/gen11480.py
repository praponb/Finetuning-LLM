from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .cube(
        F.when(F.col("amount") >= 4400, "LARGE").otherwise("SMALL").alias("amount_tier"),
        F.when(F.col("session_no") >= 7, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
        F.when(F.col("is_new_session") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_new_session_tier"),
    )
    .agg(
        F.percentile_approx("session_no", 0.5).alias("session_no_p50"),
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("merchant") == "Amazon", 1)), F.lit(0)), 2).alias("session_no_per_amazon"),
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.percentile_approx("session_no", 0.9).alias("session_no_p90"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_rows"),
    )
    .filter(F.col("shell_rows") < 1)
)
