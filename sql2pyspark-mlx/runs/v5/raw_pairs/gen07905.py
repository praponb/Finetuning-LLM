from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id")
w_running = Window.partitionBy("status").orderBy("row_seq", "id")
w4 = Window.orderBy("next_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("account_type").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("transaction_txn_type").orderBy(F.col("prev_prev_balance").desc(), "id")

with_windows = (
    df_accounts
    .select(
        "*",
        F.lead("balance").over(w_grp).alias("next_balance"),
        F.lag("balance").over(w_grp).alias("prev_balance"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.lead("balance", 2).over(w2).alias("next_balance_2"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        "next_balance",
        "prev_balance",
        "row_seq",
        F.lag("prev_balance").over(w_running).alias("prev_prev_balance"),
        F.lead("balance").over(w_running).alias("next_balance_3"),
        F.lag("balance").over(w_running).alias("prev_balance_2"),
        F.row_number().over(w_running).alias("seq_no"),
        F.sum("balance").over(w4).alias("cumulative_balance"),
    )
)

cleaned = (
    enriched
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        "row_seq",
        "prev_prev_balance",
        "next_balance_3",
        "prev_balance_2",
        "seq_no",
        "cumulative_balance",
        F.abs("prev_balance_2").alias("abs_prev_balance_2"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)

scored = (
    latest_match
    .withColumn("running_max_abs_prev_balance_2", F.max("abs_prev_balance_2").over(w6))
    .withColumn("running_min_abs_prev_balance_2", F.min("abs_prev_balance_2").over(w6))
    .withColumn("running_count_abs_prev_balance_2", F.count("abs_prev_balance_2").over(w6))
    .withColumn("row_seq_2", F.row_number().over(w7))
)

result = (
    scored
    .select(
        "id",
        "account_type",
        "status",
        "row_seq",
        "prev_prev_balance",
        "next_balance_3",
        "prev_balance_2",
        "seq_no",
        "abs_prev_balance_2",
    )
    .orderBy(F.col("status").desc_nulls_first())
)
