from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-10-20 17:00:00").cast("timestamp"))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.lower("merchant").alias("merchant_lower"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_filled"),
    )
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

kept_keys = (
    sessions_transactions
    .select("txn_type", "merchant")
    .subtract(
        df_transactions
        .filter(~F.col("txn_type").isin(["REFUND", "FEE"]))
        .select("txn_type", "merchant")
    )
)

verified = (
    sessions_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-05-22 01:00:00"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant_filled",
        F.round((F.col("amount") * 1.15), 2).alias("adjusted_amount"),
    )
)

filtered = (
    valid
    .filter(F.col("txn_type").isin(["FEE", "REFUND"]))
    .select("*", F.when(F.col("txn_type") == "FEE", 1).otherwise(0).alias("is_fee"))
)

base = (
    filtered
    .filter(F.col("adjusted_amount") <= 76)
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant_filled",
        "is_fee",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        (F.col("amount") * F.col("is_fee")).alias("amount_times_is_fee"),
    )
)

result = (
    base
    .select("amount", "merchant_filled", "is_fee", "amount_filled")
    .orderBy("amount_filled", "amount")
)
