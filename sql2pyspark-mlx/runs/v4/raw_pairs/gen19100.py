from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("txn_time", "id")
w2 = Window.partitionBy("status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id", "status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.balance"), F.col("b.status"))
)

with_gaps = (
    with_dims
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

with_windows = (
    sessions_transactions
    .select(
        "*",
        F.sum("burst_id").over(w3).alias("running_burst_id"),
        F.first("balance", ignorenulls=True).over(w4).alias("first_known_balance"),
        F.sum("amount").over(w5).alias("running_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "balance",
        "status",
        "session_start_flag",
    )
    .orderBy(F.col("merchant").asc_nulls_first())
)
