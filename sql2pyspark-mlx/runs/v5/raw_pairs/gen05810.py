from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy("opened_date", "id")
w_hist = Window.partitionBy("status").orderBy("prev_adjusted_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

joined = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.name").like("C%"))
    .select(
        "a.id",
        "a.customer_id",
        "a.balance",
        "a.opened_date",
        "a.status",
        F.col("p.signup_date"),
        F.col("p.name"),
    )
)

base = (
    joined
    .filter(F.col("status") != "FROZEN")
    .select(
        "id",
        "opened_date",
        "status",
        "signup_date",
        "name",
        F.round((F.col("balance") * 0.9), 2).alias("adjusted_balance"),
        F.coalesce("status", F.lit("OTHER")).alias("status_clean"),
    )
)

enriched = (
    base
    .withColumn("prev_adjusted_balance", F.lag("adjusted_balance").over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent))
)

enriched_accounts = (
    enriched
    .withColumn("running_min_adjusted_balance", F.min("adjusted_balance").over(w_hist))
    .withColumn("running_max_adjusted_balance", F.max("adjusted_balance").over(w_hist))
    .withColumn("running_count_adjusted_balance", F.count("adjusted_balance").over(w_hist))
    .withColumn("running_sum_adjusted_balance", F.sum("adjusted_balance").over(w_hist))
    .withColumn("status_count_adjusted_balance", F.count("adjusted_balance").over(w3))
)

with_latest = (
    enriched_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_type"),
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

grouped = (
    with_latest
    .groupBy(
        F.when(F.col("adjusted_balance") >= 76, "HIGH").otherwise("LOW").alias("adjusted_balance_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.round(F.avg("running_min_adjusted_balance"), 2).alias("avg_running_min_adjusted_balance"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
    )
)

result = (
    grouped
    .select("adjusted_balance_tier", "closed_count", "avg_running_min_adjusted_balance")
    .orderBy(F.col("avg_running_min_adjusted_balance").desc_nulls_first())
)
