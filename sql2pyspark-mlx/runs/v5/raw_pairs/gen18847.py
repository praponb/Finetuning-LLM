from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

with_latest = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.txn_time").alias("transaction_txn_time"),
    )
)

result = (
    with_latest
    .groupBy("account_type", "customer_id")
    .agg(
        F.count_if(F.col("account_type") == "CHECKING").alias("checking_events"),
        F.count_if(F.col("status") == "OPEN").alias("open_events"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .filter(F.col("checking_events") > 1)
    .orderBy(F.col("checking_events").desc_nulls_last())
)
