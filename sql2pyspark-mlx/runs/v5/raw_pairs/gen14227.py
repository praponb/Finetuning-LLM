from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "txn_type").orderBy("txn_time_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("account_type", "merchant").orderBy(F.col("amount").asc_nulls_first())
w_running = Window.partitionBy("txn_type", "account_type").orderBy("amount_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_type").orderBy("amount_percent_rank", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("txn_type", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("account_type", "account_id").orderBy("amount_percent_rank", "id")

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-07-07 00:00:00"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.to_date("txn_time").alias("txn_time_date"),
        F.substring("merchant", 1, 4).alias("merchant_prefix"),
    )
)

enriched = (
    valid
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .filter(F.col("p.customer_id") == 10)
    .select("a.*", F.col("p.opened_date"), F.col("p.account_type"))
)

enriched_transactions = (
    enriched
    .withColumn("cumulative_amount", F.sum("amount").over(w))
    .withColumn("amount_avg_90d", F.avg("amount").over(w2))
    .withColumn("amount_percent_rank", F.percent_rank().over(w3))
)

with_windows = (
    enriched_transactions
    .withColumn("cumulative_amount_sum_90d", F.sum("cumulative_amount").over(w2))
    .withColumn("first_known_account_type", F.first("account_type", ignorenulls=True).over(w_running))
    .withColumn("cumulative_amount_avg_5", F.avg("cumulative_amount").over(w5))
    .withColumn("first_known_amount_percent_rank", F.first("amount_percent_rank", ignorenulls=True).over(w6))
    .withColumn("next_amount", F.lead("amount", 1).over(w7))
)

matched_keys = (
    with_windows
    .select("merchant", "id")
    .subtract(
        df_transactions
        .filter(F.col("merchant") == "Starbucks")
        .select("merchant", "id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("account_id", "account_type")
    .pivot("txn_type", ["DEBIT", "FEE"])
    .agg(F.max("amount_avg_90d"))
)
