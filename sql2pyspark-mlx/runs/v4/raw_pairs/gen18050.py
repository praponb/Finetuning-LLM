from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_first())

filtered = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2024-02-18 23:00:00").cast("timestamp"))
    .select(
        "*",
        F.concat("merchant", F.lit(" "), "txn_type").alias("merchant_txn_type"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    deduped
    .filter(F.col("txn_time") > F.lit("2024-03-17 20:00:00").cast("timestamp"))
    .select(
        "*",
        (F.col("dedup_rank") * F.col("amount")).alias("dedup_rank_times_amount"),
        F.ceil("amount").alias("amount_whole"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "merchant_txn_type",
        "dedup_rank",
        "dedup_rank_times_amount",
        F.when(F.col("txn_type").rlike("^(DEBIT|FEE)$"), "EXTENDED").when(F.col("txn_type").rlike("^(CREDIT|REFUND)$"), "OTHER_ZONE").otherwise("CORE").alias("txn_type_zone"),
    )
)
