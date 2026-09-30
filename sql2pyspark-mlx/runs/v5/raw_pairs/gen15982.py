from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("txn_time", "id")
w_hist = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w_part = Window.partitionBy("customer_id", "transaction_merchant").orderBy("balance_bucket", "id")
w7 = Window.partitionBy("transaction_merchant").orderBy("balance_bucket", "id")

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.customer_id", "s.balance", "s.opened_date", F.col("c.txn_time"))
)

kept = (
    with_last
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.txn_time")), "left_semi")
)

with_windows = (
    kept
    .filter(F.col("opened_date") <= "2025-02-01")
    .select(
        "*",
        F.avg("balance").over(w2).alias("balance_avg_5_prior"),
        F.first("balance", ignorenulls=True).over(w3).alias("first_known_balance"),
        F.row_number().over(w4).alias("event_seq"),
        F.ntile(3).over(w_hist).alias("balance_bucket"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant").alias("transaction_merchant"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id") == 20), "left_anti")
)

eligible_accounts = (
    eligible
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.merchant") == "Starbucks"), "left_anti")
)

result = (
    eligible_accounts
    .select(
        "id",
        "opened_date",
        "txn_time",
        "first_known_balance",
        "event_seq",
        "balance_bucket",
        "transaction_merchant",
        F.lag("balance_avg_5_prior").over(w_part).alias("prev_balance_avg_5_prior"),
        F.lead("balance_avg_5_prior").over(w_part).alias("next_balance_avg_5_prior"),
        F.lag("event_seq").over(w_part).alias("prev_event_seq"),
        F.row_number().over(w_part).alias("event_seq_2"),
        F.lag("balance", 2).over(w7).alias("prev_balance"),
    )
)
