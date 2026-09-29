from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_first(), "id")

latest = (
    df_transactions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

remaining_keys = (
    latest
    .select("txn_type")
    .exceptAll(
        df_transactions
        .filter(F.col("account_id").isin([4, 6]))
        .select("txn_type")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .select(
        "*",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("resolved_merchant"),
    )
)

filtered_transactions = (
    filtered
    .filter(F.col("merchant") == "Shell")
    .select(
        "*",
        F.round((F.col("amount_filled") / F.col("amount")), 2).alias("amount_filled_per_amount"),
        F.round((F.col("amount_filled") / F.col("amount")), 2).alias("amount_filled_per_amount_2"),
    )
)

cleaned = (
    filtered_transactions
    .filter((F.col("txn_time") > F.lit("2023-10-06").cast("date")) & F.col("merchant").isNotNull())
    .select("*", F.col("amount_filled").cast("int").alias("amount_filled_int"))
)

result = (
    cleaned
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["account_type", "txn_type"], ["account_type"]],
        F.col("d.account_type").alias("account_type"),
        F.col("e.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("e.amount_filled_int")).alias("avg_amount_filled_int"),
        F.max(F.col("e.amount_filled")).alias("max_amount_filled"),
    )
    .orderBy(F.col("max_amount_filled").asc_nulls_first())
)
