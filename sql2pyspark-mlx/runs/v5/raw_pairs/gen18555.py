from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("account_type")
w3 = Window.orderBy("balance", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy("opened_date", "id")

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.opened_date",
        "s.status",
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

metrics = (
    with_last
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "transaction_txn_type",
        F.stddev_samp("balance").over(w2).alias("account_type_stddev_samp_balance"),
        F.avg("balance").over(w3).alias("balance_avg_3"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.balance",
        "s.opened_date",
        "s.account_type_stddev_samp_balance",
        "s.row_seq",
        F.col("c.credit_limit"),
    )
)

joined_accounts = (
    joined
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.credit_limit",
        F.col("p.segment"),
        F.col("p.credit_limit").alias("customer_credit_limit"),
    )
)

with_latest = (
    joined_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant").alias("transaction_merchant"))
)

result = (
    with_latest
    .groupBy("customer_id")
    .pivot("transaction_merchant", ["Shell", "7-Eleven"])
    .agg(
        F.sum("credit_limit").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "customer_id",
        F.col("Shell_value_sum").alias("shell_value_sum"),
        F.col("Shell_item_count").alias("shell_item_count"),
        F.col("7-Eleven_value_sum").alias("v_7_eleven_value_sum"),
        F.col("7-Eleven_item_count").alias("v_7_eleven_item_count"),
    )
)
