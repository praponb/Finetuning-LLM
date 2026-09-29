from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("status").orderBy("balance_adjusted", "id").rowsBetween(-2, -1)
w4 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("balance_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id", "merchant").orderBy("balance_avg_2_prior", "id")
w7 = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("txn_type").orderBy("balance", "id")
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("txn_type").orderBy("row_seq", "id")
w_part = Window.orderBy(F.col("opened_date").desc(), "id")

cleaned = (
    df_accounts
    .filter(~F.col("account_type").isin(["SAVINGS", "LOAN", "CREDIT"]))
    .withColumn("balance_adjusted", F.round((F.col("balance") * 0.85), 2))
    .withColumn("status_clean", F.coalesce("status", F.lit("N/A")))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"), F.col("c.txn_type"))
)

with_windows = (
    with_latest
    .filter(F.col("merchant") == "Amazon")
    .select(
        "*",
        F.sum("balance_adjusted").over(w2).alias("status_sum_balance_adjusted"),
        F.avg("balance").over(w3).alias("balance_avg_2_prior"),
        F.last("merchant", ignorenulls=True).over(w4).alias("last_known_merchant"),
    )
)

enriched = (
    with_windows
    .withColumn("cumulative_balance_avg_2_prior", F.sum("balance_avg_2_prior").over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("running_balance", F.sum("balance").over(w7))
    .withColumn("row_seq", F.row_number().over(w8))
)

with_last = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

with_windows_accounts = (
    with_last
    .withColumn("prev_balance_avg_2_prior", F.lag("balance_avg_2_prior", 2).over(w_recent))
    .withColumn("row_seq_2", F.row_number().over(w_part))
)

with_last_accounts = (
    with_windows_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_type").alias("transaction_txn_type"),
        F.col("c.merchant").alias("transaction_merchant_2"),
    )
)

result = (
    with_last_accounts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.segment").alias("segment"),
        F.col("s.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.seq_no")).alias("avg_seq_no"),
    )
    .orderBy(F.col("avg_seq_no").asc_nulls_first())
)
