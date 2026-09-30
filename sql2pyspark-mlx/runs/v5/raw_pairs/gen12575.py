from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

matched_keys = (
    sessionized
    .select("txn_type", "merchant")
    .intersect(
        df_transactions
        .filter(F.to_date("txn_time") >= "2023-04-07")
        .select("txn_type", "merchant")
    )
)

reconciled = (
    sessionized
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter((F.col("txn_time") > F.lit("2023-07-08").cast("date")) & ~F.col("merchant").isin(["7-Eleven", "Grab", "Amazon"]))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("OTHER")))
)

cleaned = (
    valid
    .filter((F.col("txn_time") >= F.lit("2023-08-18 07:00:00").cast("timestamp")) & (F.col("is_flagged_flag") <= 56))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "session_no",
        "is_flagged_flag",
        "merchant_clean",
        (F.col("is_flagged_flag") * F.col("session_no")).alias("is_flagged_flag_times_session_no"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

base = (
    cleaned
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("Amazon"))))
    .select(
        "*",
        (F.col("is_flagged_flag") / F.nullif("is_flagged_flag_times_session_no", F.lit(0))).alias("is_flagged_flag_per_is_flagged_flag_times_session_no"),
    )
)

kept_keys = (
    base
    .select("txn_type")
    .exceptAll(
        df_transactions
        .filter(F.col("account_id").isin([4, 36, 39]))
        .select("txn_type")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("txn_time") >= F.lit("2023-07-21").cast("date")) & (F.col("merchant").eqNullSafe(F.lit("Lazada"))))
    .withColumn("is_flagged_flag_times_session_no_rounded", F.round("is_flagged_flag_times_session_no", 0))
    .withColumn("session_no_num", F.col("session_no").cast("double"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("N/A")))
)

daily = (
    filtered
    .groupBy(
        F.when(F.col("is_flagged_flag_times_session_no") >= 74, "PREMIUM").otherwise("STANDARD").alias("is_flagged_flag_times_session_no_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.round(F.sum("is_flagged_flag_times_session_no_rounded") / F.nullif(F.count(F.when(F.col("merchant") == "Grab", 1)), F.lit(0)), 2).alias("is_flagged_flag_times_session_no_rounded_per_grab"),
        F.count_if(F.col("merchant") == "Shell").alias("shell_events"),
    )
)

result = (
    daily
    .select(
        "is_flagged_flag_times_session_no_tier",
        "span_minutes",
        "is_flagged_flag_times_session_no_rounded_per_grab",
        "shell_events",
    )
)
