from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time_hour_filled", "id")
w2 = Window.partitionBy("txn_type", "status").orderBy(F.col("balance").desc_nulls_first(), "id")

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-04-27").cast("date")) & F.col("account_id").isin([22, 25]))
    .select(
        "*",
        F.hour("txn_time").alias("txn_time_hour"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

valid = (
    cleaned
    .filter(F.col("merchant").isNotNull() & (F.col("merchant").eqNullSafe(F.lit("Shell"))))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.coalesce("txn_time_hour", F.lit(0)).alias("txn_time_hour_filled"),
        F.date_format("txn_time", "yyyy-MM").alias("txn_time_label"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    latest
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "inner")
    .filter(F.col("d.account_type") == "LOAN")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.txn_time",
        "a.merchant",
        "a.is_flagged",
        "a.txn_time_hour_filled",
        "a.txn_time_label",
        F.col("d.balance"),
        F.col("d.status"),
        F.col("d.opened_date"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged",
        "txn_time_hour_filled",
        "status",
        "opened_date",
    )
)

pivoted = (
    ranked
    .groupBy("txn_type", "status")
    .pivot("merchant", ["7-Eleven", "Starbucks", "Amazon"])
    .agg(
        F.max("txn_time_hour_filled").alias("value_sum"),
        F.count(F.lit(1)).alias("cnt"),
    )
    .select(
        "txn_type",
        "status",
        F.col("7-Eleven_value_sum").alias("v_7_eleven_value_sum"),
        F.col("7-Eleven_cnt").alias("v_7_eleven_cnt"),
        F.col("Starbucks_value_sum").alias("starbucks_value_sum"),
        F.col("Starbucks_cnt").alias("starbucks_cnt"),
        F.col("Amazon_value_sum").alias("amazon_value_sum"),
        F.col("Amazon_cnt").alias("amazon_cnt"),
    )
)

result = (
    pivoted
    .select(
        "txn_type",
        "status",
        "v_7_eleven_value_sum",
        "v_7_eleven_cnt",
        "starbucks_value_sum",
        "starbucks_cnt",
    )
    .orderBy(F.col("status").desc_nulls_first())
)
