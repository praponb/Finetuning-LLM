from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_last(), "id")
w2 = Window.partitionBy("merchant", "status").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("amount")
w_hist = Window.partitionBy("account_id", "status")
w_ordered = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("status")

with_dims = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.opened_date"), F.col("p.status"))
)

ranked = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

kept_keys = (
    ranked
    .select("merchant", "txn_type")
    .exceptAll(
        df_transactions
        .filter(F.col("account_id").isin([28, 36]))
        .select("merchant", "txn_type")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .filter(F.col("account_id").isin([6, 10, 31]))
    .select(
        "*",
        F.max("amount").over(w2).alias("amount_max_90d"),
        F.percent_rank().over(w3).alias("amount_percent_rank"),
        F.stddev_samp("amount").over(w_hist).alias("account_id_stddev_samp_amount"),
        F.max("amount").over(w_ordered).alias("amount_max_90d_2"),
        F.round((F.col("amount") / F.sum("amount").over(w6)), 4).alias("amount_share"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.status").alias("account_status"))
)

joined_transactions = (
    joined
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.balance"),
        F.col("p.opened_date").alias("account_opened_date"),
        F.col("p.account_type"),
    )
)

enriched = (
    joined_transactions
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.account_id",
        "e.amount",
        "e.txn_time",
        "e.status",
        "e.amount_max_90d",
        "e.account_id_stddev_samp_amount",
        "e.amount_max_90d_2",
        "e.account_opened_date",
        "e.account_type",
        F.col("b.status").alias("account_status_2"),
        F.col("b.account_type").alias("account_account_type"),
        F.col("b.balance").alias("account_balance"),
    )
)

result = (
    enriched
    .select("status", "amount", "account_balance")
    .unpivot(
        ids=["status"],
        values=["amount", "account_balance"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
)
