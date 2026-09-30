from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("amount").desc(), "id")
w4 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("txn_type").orderBy("amount", "id")
w6 = Window.partitionBy("txn_type", "merchant").orderBy(F.col("amount_avg_6_prior").desc(), "id")

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= "2024-01-01"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.to_date("txn_time").alias("txn_time_date"),
        F.lower("txn_type").alias("txn_type_lower"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.amount",
        "s.merchant",
        "s.txn_time_date",
        F.col("a.account_type"),
        F.col("a.status"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "account_type",
        F.first("account_type", ignorenulls=True).over(w).alias("first_known_account_type"),
        F.first("status", ignorenulls=True).over(w2).alias("first_known_status"),
    )
)

metrics = (
    with_windows
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("amount_avg_6_prior", F.avg("amount").over(w4))
    .withColumn("prev_amount", F.lag("amount", 1, 0).over(w5))
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

summary = (
    ranked
    .groupBy(
        "account_type",
        F.when(F.col("prev_amount") >= 3600, "HIGH").otherwise("LOW").alias("prev_amount_tier"),
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_count"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount_avg_6_prior")).otherwise(0)).alias("debit_amount_avg_6_prior"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_rows"),
        F.count_if(F.col("merchant") == "7-Eleven").alias("7_eleven_events"),
    )
    .filter(F.col("refund_rows") > 1)
)

result = (
    summary
    .select(
        "account_type",
        "prev_amount_tier",
        "row_count",
        "shell_count",
        "refund_rows",
    )
    .orderBy(F.col("prev_amount_tier").asc_nulls_last())
)
