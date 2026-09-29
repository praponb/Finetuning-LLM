from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-10-05 09:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.lower("merchant").alias("merchant_lower"),
    )
)

gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

remaining_keys = (
    sessions_transactions
    .select("account_id", "merchant")
    .intersect(
        df_transactions
        .filter(~F.col("is_flagged"))
        .select("account_id", "merchant")
    )
)

verified = (
    sessions_transactions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(~F.col("merchant").isin(["7-Eleven", "Shell"]))
    .select(
        "*",
        F.initcap(F.trim("merchant")).alias("merchant_title"),
        (F.col("session_no") - F.col("is_new_burst")).alias("session_no_minus_is_new_burst"),
    )
)

result = (
    filtered
    .select(
        "id",
        "account_id",
        "merchant_lower",
        "is_new_burst",
        "session_no",
        "merchant_title",
        "session_no_minus_is_new_burst",
    )
    .orderBy(F.col("account_id").asc_nulls_last())
)
