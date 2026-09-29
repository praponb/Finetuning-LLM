from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("account_id", "account_account_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("balance", "id")

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-06-24 14:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        F.concat("txn_type", F.lit(", "), "merchant").alias("txn_type_merchant"),
    )
)

joined = (
    filtered
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "inner")
    .select("a.*", F.col("p2.balance"), F.col("p2.account_type"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "inner")
    .filter(F.col("p.status") == "FROZEN")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.amount",
        "s.balance",
        "s.account_type",
        F.col("p.account_type").alias("account_account_type"),
        F.col("p.balance").alias("account_balance"),
        F.col("p.status"),
    )
)

enriched = (
    with_dims
    .select(
        "id",
        "txn_type",
        "balance",
        "account_type",
        "account_account_type",
        "status",
        F.count("balance").over(w_hist).alias("running_count_balance"),
        F.max("balance").over(w_hist).alias("running_max_balance"),
        F.avg("balance").over(w_hist).alias("running_avg_balance"),
        F.lag("amount", 1, 0).over(w_recent).alias("prev_amount"),
    )
)

result = (
    enriched
    .withColumn("balance_tier", F.when(F.col("balance") >= 81000, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["account_type", "balance_tier", "status"], ["account_type", "balance_tier"]],
        "account_type",
        "balance_tier",
        "status",
    )
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
        F.percentile_approx("running_count_balance", 0.5).alias("running_count_balance_p50"),
        F.round(F.sum("running_count_balance") / F.nullif(F.count(F.when(F.col("status") == "OPEN", 1)), F.lit(0)), 2).alias("running_count_balance_per_open"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("running_count_balance_p50").desc())
)
