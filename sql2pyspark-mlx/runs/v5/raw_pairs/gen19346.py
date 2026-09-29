from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("customer_id", "transaction_txn_type").orderBy(F.col("balance").desc(), "id")
w4 = Window.partitionBy("status", "account_type").orderBy("row_seq", "id")
w5 = Window.partitionBy("status").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("transaction_txn_type").orderBy("balance_avg_6_prior", "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "*",
        F.when(F.col("status").isin(["FROZEN", "OPEN"]), "Core").otherwise("Other").alias("status_group"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.merchant"),
        F.col("c.txn_type").alias("transaction_txn_type"),
    )
)

with_windows = (
    with_latest
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "status_group",
        "resolved_status",
        "transaction_txn_type",
        F.avg("balance").over(w_ordered).alias("balance_avg_6_prior"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "opened_date",
        F.lag("balance", 2).over(w4).alias("prev_balance"),
        F.first("status", ignorenulls=True).over(w5).alias("first_known_status"),
        F.ntile(4).over(w6).alias("balance_avg_6_prior_bucket"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date")), "left_anti")
)

with_latest_accounts = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"))
)

totals = (
    with_latest_accounts
    .groupBy(
        F.when(F.col("balance_avg_6_prior_bucket") >= 8, "LARGE").otherwise("SMALL").alias("balance_avg_6_prior_bucket_tier"),
    )
    .agg(
        F.min("transaction_txn_time").alias("first_seen"),
        F.round(F.avg("prev_balance"), 2).alias("avg_prev_balance"),
        F.round(F.avg("balance_avg_6_prior_bucket"), 2).alias("avg_balance_avg_6_prior_bucket"),
        F.count("*").alias("cnt"),
        F.max("transaction_txn_time").alias("last_transaction_txn_time"),
        F.percentile_approx("balance_avg_6_prior_bucket", 0.9).alias("balance_avg_6_prior_bucket_p90"),
    )
)

result = (
    totals
    .select(
        "balance_avg_6_prior_bucket_tier",
        "first_seen",
        "avg_prev_balance",
        "last_transaction_txn_time",
    )
)
