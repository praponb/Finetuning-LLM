from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy(F.col("txn_time").desc_nulls_last(), "id")

base = (
    df_transactions
    .filter((F.col("amount") > 1400) & (F.col("txn_time") > F.lit("2023-05-08").cast("date")))
    .withColumn("clean_merchant", F.trim("merchant"))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "account_id", "txn_type", "merchant", "clean_merchant")
)

base_transactions = (
    latest
    .filter(F.col("account_id").isin([18, 20]))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("N/A")))
)

base_transactions_2 = (
    base_transactions
    .filter(F.col("account_id").isin([3, 10, 13, 20]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "clean_merchant",
        F.concat("txn_type", F.lit(", "), "merchant").alias("txn_type_merchant"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

result = (
    base_transactions_2
    .groupBy("merchant", "account_id")
    .agg(
        F.count("*").alias("row_count"),
        F.count_if(F.col("merchant") == "Starbucks").alias("starbucks_events"),
        F.countDistinct("merchant").alias("distinct_merchants"),
        F.countDistinct("txn_type").alias("distinct_txn_types"),
    )
    .orderBy(F.col("distinct_merchants").desc())
)
