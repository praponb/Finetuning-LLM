from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id")
w2 = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_first())
w3 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_running = Window.partitionBy("merchant").orderBy(F.col("is_7_eleven").desc_nulls_last())
w_part = Window.partitionBy("account_id", "txn_type")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("is_7_eleven", F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0))
)

kept_keys = (
    filtered
    .select("merchant", "account_id")
    .intersect(
        df_transactions
        .filter(F.col("account_id").isin([20, 25, 34, 35]))
        .select("merchant", "account_id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "account_id",
        "txn_time",
        F.count("is_7_eleven").over(w).alias("account_id_count_is_7_eleven"),
        F.percent_rank().over(w2).alias("amount_percent_rank"),
        F.max("is_7_eleven").over(w3).alias("is_7_eleven_max_90d"),
        F.dense_rank().over(w_running).alias("is_7_eleven_dense_rank"),
        F.sum("is_7_eleven").over(w_part).alias("account_id_sum_is_7_eleven"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.status"))
)

with_dims_transactions = (
    with_dims
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .filter(F.month(F.col("a.opened_date")).isin([3, 6, 11]))
    .select(
        "e.account_id",
        "e.txn_time",
        "e.is_7_eleven_max_90d",
        "e.is_7_eleven_dense_rank",
        "e.account_id_sum_is_7_eleven",
        F.col("a.balance"),
    )
)

result = (
    with_dims_transactions
    .groupBy(F.date_trunc("QUARTER", "txn_time").alias("quarter_start"), "account_id")
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.min("txn_time").alias("first_seen"),
        F.max("txn_time").alias("last_txn_time"),
    )
    .orderBy(F.col("account_id").asc_nulls_last())
)
