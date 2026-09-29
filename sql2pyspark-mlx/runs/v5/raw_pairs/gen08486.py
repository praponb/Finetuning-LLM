from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("balance", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("account_type", "customer_id").orderBy("balance", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("status").orderBy(F.col("balance_avg_5_prior").asc_nulls_last(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

base = (
    df_accounts
    .filter((F.col("status").eqNullSafe(F.lit("OPEN"))) & F.col("status").isNotNull())
    .withColumn("account_type_group", F.when(F.col("account_type").isin(["CREDIT", "LOAN"]), "Tier 1").otherwise("Other"))
    .withColumn("account_type_prefix", F.substring("account_type", 1, 3))
)

with_windows = (
    base
    .select(
        "*",
        F.min("balance").over(w).alias("last7_min_balance"),
        F.count("balance").over(w).alias("last7_count_balance"),
        F.avg("balance").over(w).alias("last7_avg_balance"),
        F.avg("balance").over(w2).alias("balance_avg_5_prior"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        "account_type_prefix",
        "last7_count_balance",
        "balance_avg_5_prior",
    )
)

latest_match = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.status",
        "s.last7_count_balance",
        F.col("c.txn_time").alias("transaction_txn_time"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.last7_count_balance",
        F.col("c.txn_time").alias("transaction_txn_time_2"),
    )
)

result = (
    with_latest
    .select("id", "last7_count_balance", "transaction_txn_time_2")
    .orderBy(F.col("transaction_txn_time_2").desc())
)
