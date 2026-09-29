from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("adjusted_balance").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter((~F.col("status").eqNullSafe(F.lit("CLOSED"))) & (F.col("opened_date") >= F.lit("2023-10-07").cast("date")))
    .select(
        "*",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.round((F.col("balance") * 1.1), 2).alias("adjusted_balance"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.balance_filled",
        "s.adjusted_balance",
        "s.rn",
        F.col("c.merchant"),
        F.col("c.amount"),
    )
)

matched_keys = (
    with_latest
    .select("id")
    .subtract(
        df_accounts
        .filter(F.col("customer_id") == 30)
        .select("id")
    )
)

verified = (
    with_latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned_accounts = (
    verified
    .filter(F.col("merchant").eqNullSafe(F.lit("Shell")))
    .withColumn("rn_minus_amount", (F.col("rn") - F.col("amount")))
)

base = (
    cleaned_accounts
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .withColumn("rn_minus_adjusted_balance", (F.col("rn") - F.col("adjusted_balance")))
    .withColumn("amount_minus_adjusted_balance", (F.col("amount") - F.col("adjusted_balance")))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("NONE")))
)

result = (
    base
    .rollup(
        F.when(F.col("amount_minus_adjusted_balance") >= 69, "PREMIUM").otherwise("STANDARD").alias("amount_minus_adjusted_balance_tier"),
        F.when(F.col("balance_filled") >= 67, "HIGH").otherwise("LOW").alias("balance_filled_tier"),
    )
    .agg(
        F.count(F.when(F.col("merchant") == "7-Eleven", 1)).alias("7_eleven_rows"),
        F.percentile_approx("amount_minus_adjusted_balance", 0.9).alias("amount_minus_adjusted_balance_p90"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"),
    )
    .orderBy(F.col("balance_filled_tier").desc())
)
