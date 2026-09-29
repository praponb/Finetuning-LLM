from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("account_id", "txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("running_amount", "id")
w4 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w5 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("merchant")
w7 = Window.partitionBy("txn_type").orderBy("is_new_burst", "id")

metrics = (
    df_transactions
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.last("amount", ignorenulls=True).over(w_recent).alias("last_known_amount"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

flagged = (
    deduped
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w4) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w5))

scored = (
    sessions_transactions
    .select(
        "*",
        F.min("amount").over(w6).alias("merchant_min_amount"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

gaps = (
    scored
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w4).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w4) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions_2 = gaps.withColumn("burst_id_2", F.sum("session_start_flag").over(w5))

result = (
    sessions_transactions_2
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "running_amount",
        "is_new_burst",
    )
    .orderBy("merchant")
)
