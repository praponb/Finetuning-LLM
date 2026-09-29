from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id", "merchant").orderBy(F.col("session_no").desc(), "id")
w4 = Window.partitionBy("txn_type", "status").orderBy("session_no", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("account_id", "txn_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("account_status").orderBy(F.col("opened_date").desc(), "id")

flagged = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    flagged
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

enriched = (
    sessions_transactions
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .filter(F.month(F.col("a.opened_date")).isin([1, 3, 12]))
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.merchant",
        "s.session_no",
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("a.status"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.avg("session_no").over(w4).alias("session_no_avg_6_prior"),
        F.last("status", ignorenulls=True).over(w5).alias("last_known_status"),
        F.first("merchant", ignorenulls=True).over(w6).alias("first_known_merchant"),
    )
)

enriched_transactions = (
    metrics
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select("s.*", F.col("a.balance"), F.col("a.status").alias("account_status"))
)

ranked_transactions = (
    enriched_transactions
    .withColumn("latest_rank_2", F.row_number().over(w7))
    .filter(F.col("latest_rank_2") == 1)
)

result = (
    ranked_transactions
    .select(
        "id",
        "merchant",
        "session_no",
        "account_type",
        "opened_date",
        "status",
        "latest_rank",
        "session_no_avg_6_prior",
        "balance",
    )
)
