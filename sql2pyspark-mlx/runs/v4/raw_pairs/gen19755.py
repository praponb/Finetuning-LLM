from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w2 = Window.partitionBy("txn_type", "status")
w3 = Window.partitionBy("txn_type", "account_id")
w4 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("txn_time_hour").asc_nulls_first())
w5 = Window.partitionBy("account_id").orderBy(F.col("txn_time_hour").desc_nulls_first())
w6 = Window.partitionBy("txn_type")
w_ordered = Window.partitionBy("account_type")
w8 = Window.partitionBy("status")
w9 = Window.partitionBy("account_status")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("txn_type").isin(["REFUND", "DEBIT", "CREDIT"]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        F.hour("txn_time").alias("txn_time_hour"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.opened_date"), F.col("b.status"), F.col("b.balance"))
)

kept_keys = (
    enriched
    .select("txn_type")
    .subtract(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("txn_type")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

scored = (
    verified
    .withColumn("account_id_stddev_samp_txn_time_hour", F.stddev_samp("txn_time_hour").over(w))
    .withColumn("txn_type_avg_amount", F.avg("amount").over(w2))
    .withColumn("txn_type_max_amount", F.max("amount").over(w3))
    .withColumn("txn_time_hour_dense_rank", F.dense_rank().over(w4))
)

enriched_transactions = (
    scored
    .withColumn("txn_time_hour_rank", F.rank().over(w5))
    .withColumn("txn_type_count_txn_time_hour_dense_rank", F.count("txn_time_hour_dense_rank").over(w6))
)

enriched_transactions_2 = (
    enriched_transactions
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.account_type"), F.col("b.status").alias("account_status"))
)

result = (
    enriched_transactions_2
    .select(
        "*",
        F.count("txn_time_hour_rank").over(w_ordered).alias("account_type_count_txn_time_hour_rank"),
        F.min("account_id_stddev_samp_txn_time_hour").over(w8).alias("status_min_account_id_stddev_samp_txn_time_hour"),
        F.min("txn_time_hour").over(w9).alias("account_status_min_txn_time_hour"),
        F.max("txn_type_avg_amount").over(w6).alias("txn_type_max_txn_type_avg_amount"),
    )
)
