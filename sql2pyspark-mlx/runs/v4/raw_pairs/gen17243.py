from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").asc_nulls_last(), "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant", "account_id").orderBy("rn", "id")

filtered = (
    df_transactions
    .filter(F.col("account_id").isin([11, 16, 18]))
    .withColumn("txn_time_date", F.to_date("txn_time"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("NONE")))
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

prev_events = ranked.withColumn("prev_txn_time", F.lag("txn_time").over(w2))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("session_start_flag").over(w3))

latest = (
    sessionized
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "txn_time_date",
        "merchant_clean",
        "rn",
        "prev_txn_time",
    )
    .orderBy(F.col("prev_txn_time").desc_nulls_last())
)
