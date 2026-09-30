from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("amount", "id")
w2 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("account_id").orderBy("is_flagged_flag", "id")
w4 = Window.partitionBy("txn_type")
w5 = Window.partitionBy("account_id", "txn_type").orderBy("txn_time", "id")
w6 = Window.partitionBy("txn_type", "account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.partitionBy("account_id").orderBy("next_amount", "id")
w8 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w9 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("txn_type").orderBy("island_id", "id").rowsBetween(-6, Window.currentRow)
w_part = Window.orderBy("amount_sum_7d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("txn_type").orderBy(F.col("island_id").desc(), "id")
w13 = Window.partitionBy("txn_type").orderBy("rn")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

metrics = (
    filtered
    .filter(F.col("merchant") != "7-Eleven")
    .select(
        "*",
        F.lag("is_flagged_flag", 1, 0).over(w).alias("prev_is_flagged_flag"),
        F.avg("amount").over(w2).alias("amount_avg_3_prior"),
        F.ntile(10).over(w3).alias("is_flagged_flag_bucket"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
        F.lead("amount", 1).over(w5).alias("next_amount"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "txn_type",
        "txn_time",
        "is_flagged",
        "is_flagged_flag",
        "merchant_clean",
        "prev_is_flagged_flag",
        "amount_avg_3_prior",
        "is_flagged_flag_bucket",
        "next_amount",
        F.sum("amount").over(w6).alias("amount_sum_7d"),
        F.lag("amount", 1).over(w7).alias("prev_amount"),
    )
)

gaps = (
    scored
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w8) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "txn_type",
        "txn_time",
        "is_flagged",
        "amount_sum_7d",
        "prev_amount",
        F.sum("is_new_session").over(w9).alias("island_id"),
    )
)

enriched = (
    islands
    .select(
        "*",
        F.sum("amount_sum_7d").over(w_recent).alias("last7_sum_amount_sum_7d"),
        F.max("amount_sum_7d").over(w_recent).alias("last7_max_amount_sum_7d"),
        F.count("amount_sum_7d").over(w_recent).alias("last7_count_amount_sum_7d"),
        F.first("amount_sum_7d", ignorenulls=True).over(w_part).alias("first_known_amount_sum_7d"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w12))
    .filter(F.col("rn") == 1)
)

latest_transactions = (
    latest
    .withColumn("row_num", F.rank().over(w13))
    .filter(F.col("row_num") == 1)
)

result = (
    latest_transactions
    .groupBy("txn_type")
    .agg(
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
        F.max("txn_time").alias("last_seen"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.count_if(F.col("txn_type") == "DEBIT").alias("debit_events"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types_2"),
        F.min("txn_time").alias("first_txn_time"),
    )
    .filter(F.col("refund_events") >= 1)
    .orderBy(F.col("num_txn_types_2").desc())
)
