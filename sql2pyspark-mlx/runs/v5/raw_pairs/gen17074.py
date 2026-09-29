from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("account_type", "merchant").orderBy("balance", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("account_type").orderBy("balance", "id")
w3 = Window.partitionBy("account_account_type").orderBy("seq_no", "id")

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        F.length("merchant").alias("merchant_length"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select("s.*", F.col("a.status"), F.col("a.account_type"), F.col("a.balance"))
)

metrics = (
    joined
    .select(
        "id",
        "account_id",
        "txn_time",
        "account_type",
        "balance",
        F.count("amount").over(w_ordered).alias("last7_count_amount"),
        F.max("amount").over(w_ordered).alias("last7_max_amount"),
        F.sum("amount").over(w_ordered).alias("last7_sum_amount"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_accounts.alias("d"), F.col("s.account_id") == F.col("d.id"), "left")
    .filter(F.col("d.account_type").isin(["LOAN", "CHECKING", "CREDIT"]))
    .select(
        "s.id",
        "s.account_id",
        "s.txn_time",
        "s.balance",
        "s.last7_count_amount",
        "s.last7_sum_amount",
        "s.seq_no",
        F.col("d.opened_date"),
        F.col("d.account_type").alias("account_account_type"),
        F.col("d.status"),
    )
)

metrics_transactions = (
    enriched
    .select(
        "id",
        "balance",
        "last7_count_amount",
        "opened_date",
        "status",
        F.lead("seq_no").over(w3).alias("next_seq_no"),
        F.lead("last7_count_amount").over(w3).alias("next_last7_count_amount"),
        F.row_number().over(w3).alias("seq_no_2"),
    )
)

result = (
    metrics_transactions
    .withColumn("seq_no_2_tier", F.when(F.col("seq_no_2") >= 7, "LARGE").otherwise("SMALL"))
    .rollup("seq_no_2_tier")
    .agg(
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_rows"),
        F.percentile_approx("next_last7_count_amount", 0.95).alias("next_last7_count_amount_p95"),
        F.round(F.sum("seq_no_2") / F.nullif(F.count(F.when(F.col("status") == "FROZEN", 1)), F.lit(0)), 2).alias("seq_no_2_per_frozen"),
        F.grouping("seq_no_2_tier").alias("grouping_id"),
    )
)
