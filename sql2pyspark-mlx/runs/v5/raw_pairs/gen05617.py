from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("balance").desc())
w3 = Window.partitionBy("account_type").orderBy(F.col("balance_dense_rank").desc_nulls_last(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

metrics = (
    df_accounts
    .filter(F.col("customer_id") == 31)
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.max("balance").over(w).alias("running_max_balance"),
        F.sum("balance").over(w).alias("running_sum_balance"),
        F.avg("balance").over(w).alias("running_avg_balance"),
        F.min("balance").over(w).alias("running_min_balance"),
        F.dense_rank().over(w2).alias("balance_dense_rank"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.running_sum_balance",
        "s.running_avg_balance",
        "s.running_min_balance",
        "s.latest_rank",
        F.col("c.amount"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

base = (
    with_last
    .filter(F.col("running_min_balance").between(29000, 49000))
    .withColumn("amount_int", F.col("amount").cast("int"))
    .withColumn("transaction_txn_type_filled", F.coalesce("transaction_txn_type", F.lit("N/A")))
)

latest_match = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.running_sum_balance",
        "s.running_avg_balance",
        "s.latest_rank",
        "s.amount",
        "s.transaction_txn_type_filled",
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

result = (
    latest_match
    .groupBy("transaction_merchant", "customer_id")
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count(F.when(F.col("transaction_merchant") == "Amazon", 1)).alias("amazon_count"),
        F.sum("amount").alias("total_amount"),
        F.count(F.when(F.col("transaction_merchant") == "Amazon", 1)).alias("amazon_count_2"),
        F.count(F.when(F.col("transaction_merchant") == "Grab", 1)).alias("grab_count"),
    )
    .filter(F.col("grab_count") > 1)
)
