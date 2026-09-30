from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy("is_flagged_flag_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("merchant").eqNullSafe(F.lit("Grab"))) & (F.col("txn_time") > F.lit("2023-08-19 06:00:00").cast("timestamp")))
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        F.trim("merchant").alias("clean_merchant"),
    )
)

valid = (
    base
    .filter(F.col("merchant") == "Amazon")
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        "clean_merchant",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

filtered = (
    valid
    .filter(F.col("amount") < 1900)
    .withColumn("amount_whole", F.ceil("amount"))
    .withColumn("amount_plus_is_flagged_flag", (F.col("amount") + F.col("is_flagged_flag")))
)

cleaned = (
    filtered
    .filter(F.col("amount_plus_is_flagged_flag") >= 27)
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        "is_flagged_flag",
        "merchant_clean",
        "amount_whole",
        "amount_plus_is_flagged_flag",
        F.col("is_flagged").cast("int").alias("is_flagged_flag_2"),
        (F.col("amount_whole") + F.col("amount")).alias("amount_whole_plus_amount"),
    )
)

metrics = (
    cleaned
    .filter(F.col("is_flagged_flag_2") < 78)
    .select(
        "*",
        F.sum("amount_plus_is_flagged_flag").over(w).alias("cumulative_amount_plus_is_flagged_flag"),
        F.sum("amount_plus_is_flagged_flag").over(w).alias("running_amount_plus_is_flagged_flag"),
        F.first("amount_whole", ignorenulls=True).over(w2).alias("first_known_amount_whole"),
    )
)

result = (
    metrics
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .cube(F.col("a.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.running_amount_plus_is_flagged_flag")).alias("max_running_amount_plus_is_flagged_flag"),
        F.min(F.col("s.amount_plus_is_flagged_flag")).alias("min_amount_plus_is_flagged_flag"),
    )
)
