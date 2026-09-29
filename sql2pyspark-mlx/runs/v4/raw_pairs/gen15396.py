from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_flagged_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type", "merchant").orderBy(F.col("is_flagged_flag").desc(), "id")
w5 = Window.partitionBy("account_id", "txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("txn_type", "account_id")
w7 = Window.partitionBy("account_id").orderBy(F.col("txn_time_hour").desc(), "id")
w8 = Window.partitionBy("account_id").orderBy(F.col("first_known_is_flagged_flag").desc(), "id")
w9 = Window.partitionBy("txn_type").orderBy("first_known_is_flagged_flag", "id")
w10 = Window.partitionBy("txn_type", "account_id").orderBy("first_known_is_flagged_flag", "id")
w11 = Window.orderBy("row_seq_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-11-08").cast("date")) & F.col("merchant").isNotNull() & F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.hour("txn_time").alias("txn_time_hour"),
        F.lower("merchant").alias("merchant_lower"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_filled"),
    )
)

cleaned = (
    valid
    .filter(F.col("merchant").isNotNull() & F.col("amount").between(1900, 3700))
    .select(
        "*",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled_2"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.first("is_flagged_flag", ignorenulls=True).over(w).alias("first_known_is_flagged_flag"),
        F.last("is_flagged_flag", ignorenulls=True).over(w2).alias("last_known_is_flagged_flag"),
        F.last("merchant", ignorenulls=True).over(w3).alias("last_known_merchant"),
        F.row_number().over(w4).alias("row_seq"),
        F.sum("is_flagged_flag").over(w5).alias("cumulative_is_flagged_flag"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.round((F.col("row_seq") / F.sum("row_seq").over(w6)), 4).alias("row_seq_share"),
        F.ntile(4).over(w7).alias("txn_time_hour_bucket"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.lead("txn_time_hour", 1).over(w9).alias("next_txn_time_hour"),
        F.ntile(3).over(w10).alias("first_known_is_flagged_flag_bucket"),
        F.last("row_seq", ignorenulls=True).over(w11).alias("last_known_row_seq"),
    )
)

result = (
    metrics
    .withColumn("first_known_is_flagged_flag_bucket_tier", F.when(F.col("first_known_is_flagged_flag_bucket") >= 7, "HIGH").otherwise("LOW"))
    .withColumn("last_known_row_seq_tier", F.when(F.col("last_known_row_seq") >= 8, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["merchant", "first_known_is_flagged_flag_bucket_tier", "last_known_row_seq_tier"], ["merchant", "first_known_is_flagged_flag_bucket_tier"], ["merchant"], []],
        "merchant",
        "first_known_is_flagged_flag_bucket_tier",
        "last_known_row_seq_tier",
    )
    .agg(
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types_2"),
        F.count_if(F.col("merchant") == "Grab").alias("grab_events"),
    )
    .filter(F.col("num_txn_types") >= 1)
)
