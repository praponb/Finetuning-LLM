from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("is_flagged_flag").desc_nulls_first())
w4 = Window.orderBy(F.col("session_id").desc())
w_part = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_recent = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w7 = Window.orderBy(F.col("is_flagged_flag_cume_dist").desc_nulls_first())
w8 = Window.orderBy(F.col("is_flagged_flag_avg_30d").desc_nulls_first())

base = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2024-05-06").cast("date"))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.initcap(F.trim("merchant")).alias("merchant_title"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_clean"),
    )
)

flagged = (
    base
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

remaining_keys = (
    islands
    .select("account_id", "id")
    .subtract(
        df_transactions
        .filter(F.hour("txn_time").between(10, 15))
        .select("account_id", "id")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys = (
    kept_rows
    .select("merchant", "id")
    .subtract(
        df_transactions
        .filter(F.col("txn_time") >= "2024-04-29")
        .select("merchant", "id")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_time",
        "merchant_clean",
        "session_id",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

with_windows = (
    valid
    .withColumn("is_flagged_flag_cume_dist", F.cume_dist().over(w_hist))
    .withColumn("session_id_cume_dist", F.cume_dist().over(w4))
    .withColumn("is_flagged_flag_max_30d", F.max("is_flagged_flag").over(w_part))
    .withColumn("is_flagged_flag_avg_30d", F.avg("is_flagged_flag").over(w_part))
    .withColumn("is_flagged_flag_sum_90d", F.sum("is_flagged_flag").over(w_recent))
)

with_windows_transactions = (
    with_windows
    .select(
        "*",
        F.avg("is_flagged_flag_max_30d").over(w_recent).alias("is_flagged_flag_max_30d_avg_90d"),
        F.dense_rank().over(w7).alias("is_flagged_flag_cume_dist_dense_rank"),
        F.rank().over(w8).alias("is_flagged_flag_avg_30d_rank"),
    )
)

result = (
    with_windows_transactions
    .groupBy("session_id")
    .agg(
        F.max("txn_time").alias("last_txn_time"),
        F.count("*").alias("row_count"),
        F.round(F.avg("is_flagged_flag_avg_30d"), 2).alias("avg_is_flagged_flag_avg_30d"),
    )
)
