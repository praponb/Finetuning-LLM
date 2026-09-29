from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("opened_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("account_type", "account_id").orderBy("dedup_rank", "id")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        F.month("txn_time").alias("txn_time_month"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_filled"),
    )
)

with_dims = (
    filtered
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.account_type") != "SAVINGS")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.amount",
        "s.merchant",
        "s.merchant_filled",
        F.col("a.account_type"),
        F.col("a.opened_date"),
    )
)

ranked = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    ranked
    .withColumn("last7_sum_amount", F.sum("amount").over(w2))
    .withColumn("last7_min_amount", F.min("amount").over(w2))
    .withColumn("last7_avg_amount", F.avg("amount").over(w2))
    .withColumn("next_amount", F.lead("amount", 1).over(w3))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("last7_sum_amount") >= 16600, "LARGE").otherwise("SMALL").alias("last7_sum_amount_tier"),
    )
    .agg(
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_rows"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_rows"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("next_amount")).otherwise(0)).alias("checking_next_amount"),
    )
)
