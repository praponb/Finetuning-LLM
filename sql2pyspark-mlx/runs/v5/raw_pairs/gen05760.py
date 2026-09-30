from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy(F.col("fee_rows").desc())

flagged = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_transactions = (
    flagged
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessions_transactions
    .groupBy(
        "account_id",
        F.when(F.col("session_id") >= 7, "LARGE").otherwise("SMALL").alias("session_id_tier"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
        F.percentile_approx("amount", 0.9).alias("amount_p90"),
    )
    .withColumn("fee_rows_rank", F.rank().over(w3))
)
