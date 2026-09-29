from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount_whole").asc_nulls_first(), "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy(F.col("session_id").desc())
w5 = Window.partitionBy("account_id").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("txn_type").orderBy("first_known_amount", "id")
w8 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w9 = Window.partitionBy("merchant").orderBy("first_known_amount", "id")

filtered = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-10-14 03:00:00").cast("timestamp")) & F.col("merchant").isin(["Lazada", "7-Eleven"]))
    .select(
        "*",
        F.upper("merchant").alias("merchant_upper"),
        F.ceil("amount").alias("amount_whole"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_gaps = (
    ranked
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w3))

enriched = (
    sessionized
    .select(
        "*",
        F.rank().over(w4).alias("session_id_rank"),
        F.first("amount", ignorenulls=True).over(w5).alias("first_known_amount"),
        F.sum("is_new_burst").over(w_part).alias("running_is_new_burst"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.lag("amount", 1).over(w7).alias("prev_amount"),
        F.avg("session_id_rank").over(w8).alias("session_id_rank_avg_30d"),
        F.lag("amount_whole", 1).over(w9).alias("prev_amount_whole"),
    )
)

valid = (
    metrics
    .filter((F.col("txn_time") > F.lit("2024-04-01").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "merchant_upper",
        "amount_whole",
        "is_new_burst",
        "session_id",
        "first_known_amount",
        "prev_amount",
        "session_id_rank_avg_30d",
        (F.col("amount") - F.col("prev_amount_whole")).alias("amount_minus_prev_amount_whole"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
)

base = (
    valid
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.round("amount_minus_prev_amount_whole", 2).alias("amount_minus_prev_amount_whole_rounded"),
    )
)

result = (
    base
    .select(
        "id",
        "txn_type",
        "amount",
        "is_flagged",
        "merchant_upper",
        "amount_whole",
        "session_id",
        "first_known_amount",
        "prev_amount",
        F.when(F.col("txn_type").rlike("^(DEBIT|REFUND)$"), "CORE").otherwise("EXTENDED").alias("txn_type_zone"),
    )
    .orderBy(F.col("amount_whole").asc_nulls_last(), F.col("txn_type").desc())
)
