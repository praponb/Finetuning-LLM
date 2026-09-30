from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.balance",
        "s.opened_date",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.amount"),
    )
)

result = (
    with_last
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .groupingSets(
        [["account_type", "quarter_start"], ["account_type"], []],
        "account_type",
        "quarter_start",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("transaction_merchant").alias("distinct_transaction_merchants"),
        F.round(F.avg("amount"), 2).alias("avg_amount_2"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.grouping("account_type").alias("gid"),
    )
)
