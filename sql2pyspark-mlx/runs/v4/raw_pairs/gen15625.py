from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_transactions.withColumn("prev_txn_time", F.lag("txn_time").over(w))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "session_no",
    )
)
