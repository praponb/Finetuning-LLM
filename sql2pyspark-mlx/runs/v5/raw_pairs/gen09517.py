from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("account_id", "txn_type")
w_hist = Window.partitionBy("account_id").orderBy(F.col("amount_per_amount_int").asc_nulls_first())
w_recent = Window.partitionBy("account_id")

filtered = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-04-20").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.col("amount").cast("int").alias("amount_int"),
        F.date_format("txn_time", "yyyy-MM-dd").alias("txn_time_label"),
    )
)

gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

kept_keys = (
    sessions_transactions
    .select("account_id", "txn_type")
    .subtract(
        df_transactions
        .filter(F.col("txn_time") < "2024-01-01")
        .select("account_id", "txn_type")
    )
)

reconciled = (
    sessions_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("account_id").isin([14, 36]))
    .select(
        "*",
        (F.col("amount") / F.nullif("amount_int", F.lit(0))).alias("amount_per_amount_int"),
        F.least("amount_int", "amount").alias("least_amount_int_amount"),
    )
)

with_windows = (
    base
    .select(
        "amount",
        "txn_time",
        "txn_time_label",
        "session_id",
        "amount_per_amount_int",
        F.sum("amount_per_amount_int").over(w3).alias("amount_per_amount_int_sum_30d"),
        F.max("least_amount_int_amount").over(w4).alias("least_amount_int_amount_max_7d"),
        F.count("least_amount_int_amount").over(w5).alias("account_id_count_least_amount_int_amount"),
        F.dense_rank().over(w_hist).alias("amount_per_amount_int_dense_rank"),
        F.stddev_samp("amount_per_amount_int").over(w_recent).alias("account_id_stddev_samp_amount_per_amount_int"),
    )
)

filtered_transactions = (
    with_windows
    .filter(F.col("txn_time") >= F.lit("2023-05-29 17:00:00").cast("timestamp"))
    .select(
        "amount",
        "txn_time",
        "txn_time_label",
        "session_id",
        "amount_per_amount_int",
        "amount_per_amount_int_sum_30d",
        "account_id_stddev_samp_amount_per_amount_int",
        F.round("session_id", 1).alias("session_id_rounded"),
    )
)

result = (
    filtered_transactions
    .select(
        "amount",
        "txn_time",
        "txn_time_label",
        "amount_per_amount_int",
        "session_id_rounded",
    )
)
