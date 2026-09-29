from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2024-05-21 21:00:00").cast("timestamp")) & F.col("account_id").isin([14, 17, 38]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        F.upper("merchant").alias("merchant_upper"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .select("a.*", F.col("p2.account_type"), F.col("p2.balance"))
)

joined = (
    enriched
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.opened_date"),
        F.col("p.status"),
        F.col("p.balance").alias("account_balance"),
    )
)

result = (
    joined
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("activity_month"))
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.max("txn_time").alias("last_seen"),
        F.count_if(F.col("txn_type") == "DEBIT").alias("debit_events"),
        F.sum("account_balance").alias("total_account_balance"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("account_balance"))).alias("frozen_total_account_balance"),
        F.max(F.when(F.col("account_type") == "CREDIT", F.col("account_balance"))).alias("credit_max_account_balance"),
    )
    .orderBy(F.col("avg_balance").asc_nulls_first())
)
