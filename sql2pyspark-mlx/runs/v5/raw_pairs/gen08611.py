from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(-6, -1)
w3 = Window.orderBy(F.col("balance").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("account_type").orderBy("balance_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-02-24").cast("date")) & F.col("status").isNotNull())
    .select(
        "*",
        F.when(F.col("balance") >= 75000, "High").when(F.col("balance") >= 20000, "Medium").otherwise("Low").alias("balance_band"),
        F.coalesce("status", F.lit("NONE")).alias("status_clean"),
    )
)

enriched = (
    base
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance_band",
        F.max("balance").over(w).alias("status_max_balance"),
        F.avg("balance").over(w2).alias("balance_avg_6_prior"),
        F.ntile(4).over(w3).alias("balance_bucket"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.balance_band",
        "s.balance_avg_6_prior",
        "s.balance_bucket",
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

metrics = (
    with_latest
    .select(
        "*",
        F.count("balance_bucket").over(w5).alias("running_count_balance_bucket"),
        F.sum("balance_bucket").over(w5).alias("running_sum_balance_bucket"),
        F.max("balance_bucket").over(w5).alias("running_max_balance_bucket"),
        F.min("balance_bucket").over(w5).alias("running_min_balance_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "account_type",
        "balance_band",
        "transaction_txn_type",
        "running_count_balance_bucket",
        "running_sum_balance_bucket",
        "running_min_balance_bucket",
    )
    .orderBy(F.col("running_count_balance_bucket").desc())
)
