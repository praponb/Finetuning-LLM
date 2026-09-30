from pyspark.sql import functions as F

eligible = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_semi")
)

result = eligible.select("id", "account_type", "balance", "opened_date")
