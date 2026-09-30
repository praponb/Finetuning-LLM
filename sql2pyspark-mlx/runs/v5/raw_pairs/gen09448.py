from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_first())
w3 = Window.partitionBy("account_id").orderBy(F.col("amount_rank").desc())
w4 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.min("amount").over(w).alias("running_min_amount"),
        F.rank().over(w2).alias("amount_rank"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .filter(F.col("p.account_type").isin(["SAVINGS", "CREDIT", "LOAN"]))
    .select(
        "s.id",
        "s.account_id",
        "s.merchant",
        "s.amount_rank",
        F.col("p.opened_date"),
        F.col("p.account_type"),
        F.col("p.status"),
    )
)

joined = (
    with_dims
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.account_id",
        "e.merchant",
        "e.amount_rank",
        "e.opened_date",
        "e.status",
        F.col("p.opened_date").alias("account_opened_date"),
        F.col("p.account_type").alias("account_account_type"),
        F.col("p.balance"),
    )
)

metrics_transactions = (
    joined
    .select(
        "*",
        F.percent_rank().over(w3).alias("amount_rank_percent_rank"),
        F.first("balance", ignorenulls=True).over(w4).alias("first_known_balance"),
    )
)

result = (
    metrics_transactions
    .alias("e")
    .join(df_accounts.alias("dim"), F.col("e.account_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["account_status", "account_account_type"], ["account_status"]],
        F.col("dim.status").alias("account_status"),
        F.col("e.account_account_type").alias("account_account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("e.amount_rank")), 2).alias("avg_amount_rank"),
        F.max(F.col("e.amount_rank_percent_rank")).alias("max_amount_rank_percent_rank"),
    )
)
