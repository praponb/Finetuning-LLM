from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("is_flagged_flag").asc_nulls_last(), "id")
w4 = Window.partitionBy("txn_type").orderBy(F.col("is_flagged_flag").desc_nulls_first(), "id")

cleaned = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-03-01")
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.substring("merchant", 1, 2).alias("merchant_prefix"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_clean"),
    )
)

with_gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "merchant_prefix",
        "is_flagged_flag",
        "merchant_clean",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

deduped = (
    islands
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

with_gaps_2 = (
    ranked
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    with_gaps_2
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "merchant_prefix",
        "is_flagged_flag",
        "merchant_clean",
        "session_id",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    sessions_transactions
    .groupBy("txn_type")
    .agg(
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.avg("is_flagged_flag").alias("avg_is_flagged_flag"),
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("txn_type") == "REFUND", 1)), F.lit(0)), 2).alias("island_id_per_refund"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
    )
    .orderBy(F.col("fee_count").desc_nulls_last())
)
