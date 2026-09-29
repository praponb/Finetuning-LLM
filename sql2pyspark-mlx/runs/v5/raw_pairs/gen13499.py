from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy(F.col("running_max_amount").asc_nulls_last(), "id")
w3 = Window.partitionBy("txn_type").orderBy(F.col("running_sum_amount_filled").asc_nulls_first(), "id")
w4 = Window.partitionBy("txn_type").orderBy("running_sum_amount_filled", "id")

valid = (
    df_transactions
    .filter(F.col("amount") > 2500)
    .select(
        "*",
        F.initcap(F.trim("txn_type")).alias("txn_type_title"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

cleaned = (
    valid
    .filter(F.col("txn_time") > F.lit("2023-08-09").cast("date"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "txn_type_title",
        "merchant_clean",
        F.substring("txn_type", 1, 3).alias("txn_type_prefix"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("amount").between(3700, 4000))
    .select(
        "id",
        "txn_type",
        "is_flagged",
        "txn_type_title",
        "resolved_merchant",
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.min("amount").over(w).alias("running_min_amount"),
        F.count("amount").over(w).alias("running_count_amount"),
    )
)

cleaned_transactions = (
    with_windows
    .filter(F.col("txn_type") == "FEE")
    .withColumn("running_count_amount_level", F.when(F.col("running_count_amount") >= 14, "High").when(F.col("running_count_amount") >= 4, "Medium").otherwise("Low"))
    .withColumn("running_sum_amount_filled", F.coalesce("running_sum_amount", F.lit(0)))
)

deduped = (
    cleaned_transactions
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

deduped_transactions = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    deduped_transactions
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "txn_type_title",
        "resolved_merchant",
        "running_max_amount",
        "running_min_amount",
        "running_count_amount",
        "running_sum_amount_filled",
    )
)

result = (
    latest
    .select(
        "id",
        "txn_type_title",
        "running_max_amount",
        "running_min_amount",
        "running_count_amount",
        "running_sum_amount_filled",
    )
)
