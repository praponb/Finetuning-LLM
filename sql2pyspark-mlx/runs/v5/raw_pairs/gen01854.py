from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "txn_type").orderBy(F.col("amount").asc_nulls_last())
w2 = Window.partitionBy("txn_type", "merchant").orderBy("amount", "id")
w3 = Window.partitionBy("txn_type")
w_running = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id")
w5 = Window.partitionBy("txn_type").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("amount_percent_rank", "id")
w8 = Window.partitionBy("account_type").orderBy(F.col("amount_percent_rank").desc(), "id")
w9 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)
w10 = Window.partitionBy("account_type").orderBy(F.col("running_seq_no").desc(), "id")
w11 = Window.partitionBy("account_type").orderBy(F.col("amount_percent_rank").desc_nulls_last(), "id")

enriched = (
    df_transactions
    .select(
        "*",
        F.percent_rank().over(w).alias("amount_percent_rank"),
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)

enriched_transactions = (
    enriched
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.txn_time",
        "s.merchant",
        "s.is_flagged",
        "s.amount_percent_rank",
        "s.seq_no",
        F.col("a.account_type"),
        F.col("a.balance"),
    )
)

scored = (
    enriched_transactions
    .select(
        "*",
        F.sum("seq_no").over(w5).alias("running_seq_no"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w3)), 4).alias("seq_no_share"),
        F.first("seq_no", ignorenulls=True).over(w_hist).alias("first_known_seq_no"),
        F.ntile(10).over(w7).alias("amount_percent_rank_bucket"),
        F.row_number().over(w8).alias("row_seq"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "txn_time",
        "amount_percent_rank",
        "account_type",
        "running_seq_no",
        F.min("running_seq_no").over(w9).alias("last7_min_running_seq_no"),
        F.max("running_seq_no").over(w9).alias("last7_max_running_seq_no"),
        F.avg("running_seq_no").over(w9).alias("last7_avg_running_seq_no"),
        F.sum("running_seq_no").over(w9).alias("last7_sum_running_seq_no"),
        F.ntile(3).over(w10).alias("running_seq_no_bucket"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w11))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

base = (
    latest
    .filter((F.col("txn_time") > F.lit("2023-05-03 12:00:00").cast("timestamp")) & F.col("account_type").isin(["SAVINGS", "LOAN", "CHECKING"]))
    .select(
        "*",
        F.greatest("running_seq_no", "last7_sum_running_seq_no").alias("greatest_running_seq_no_last7_sum_running_seq_no"),
        F.lower("account_type").alias("account_type_lower"),
        F.coalesce("account_type", F.lit("UNKNOWN")).alias("resolved_account_type"),
    )
)

stats = (
    base
    .withColumn("activity_month", F.date_trunc("MONTH", "txn_time"))
    .withColumn("running_seq_no_tier", F.when(F.col("running_seq_no") >= 45, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["activity_month", "running_seq_no_tier"], ["activity_month"], []],
        "activity_month",
        "running_seq_no_tier",
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("running_seq_no")).otherwise(0)).alias("checking_running_seq_no"),
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_count"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("cnt") >= 1)
)

result = (
    stats
    .select("checking_running_seq_no", "span_minutes", "cnt")
    .orderBy(F.col("checking_running_seq_no").desc())
)
