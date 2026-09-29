from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount").desc_nulls_last(), "id")

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2024-02-04 23:00:00").cast("timestamp")) & F.col("merchant").isNotNull() & (F.col("txn_type") == "DEBIT"))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.initcap(F.trim("txn_type")).alias("txn_type_title"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_clean"),
    )
)

deduped = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    deduped
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.balance"))
)

filtered = (
    joined
    .filter((F.col("txn_time") >= F.lit("2023-06-29").cast("date")) & F.col("merchant").isNotNull() & F.col("is_flagged"))
    .withColumn("balance_times_amount", (F.col("balance") * F.col("amount")))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("UNKNOWN")))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("balance_times_amount") >= 65, "LARGE").otherwise("SMALL").alias("balance_times_amount_tier"),
    )
    .agg(
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("txn_time").alias("first_seen"),
    )
)
