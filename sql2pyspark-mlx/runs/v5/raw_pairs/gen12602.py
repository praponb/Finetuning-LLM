from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy(F.col("last3_avg_new_group_flag").desc_nulls_first(), "id")
w5 = Window.orderBy(F.col("last3_min_new_group_flag_whole").desc(), "id")
w6 = Window.orderBy("last3_min_new_group_flag_whole", "id")
w7 = Window.orderBy("last3_min_new_group_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("last3_min_new_group_flag").desc_nulls_first())
w9 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w10 = Window.orderBy("first_known_last3_min_new_group_flag_whole", "id")
w11 = Window.orderBy(F.col("last3_min_new_group_flag_whole_bucket").asc_nulls_last())
w12 = Window.orderBy(F.col("last3_min_new_group_flag_whole_count_7d").desc(), "id")

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_type") == "FEE"))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        F.date_format("txn_time", "MMM yyyy").alias("txn_time_label"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

metrics = (
    sessions_transactions
    .filter(F.col("burst_id") != 4)
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant_filled",
        F.min("new_group_flag").over(w3).alias("last3_min_new_group_flag"),
        F.count("new_group_flag").over(w3).alias("last3_count_new_group_flag"),
        F.avg("new_group_flag").over(w3).alias("last3_avg_new_group_flag"),
    )
)

deduped = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "txn_time", "merchant_filled", "last3_min_new_group_flag")
)

cleaned_transactions = (
    deduped
    .filter(F.col("txn_time") >= F.to_timestamp(F.lit("2023-09-27 04:00:00")))
    .select(
        "*",
        F.ceil("last3_min_new_group_flag").alias("last3_min_new_group_flag_whole"),
    )
)

metrics_transactions = (
    cleaned_transactions
    .select(
        "id",
        "last3_min_new_group_flag",
        "last3_min_new_group_flag_whole",
        F.ntile(4).over(w5).alias("last3_min_new_group_flag_whole_bucket"),
        F.row_number().over(w6).alias("row_seq"),
        F.first("last3_min_new_group_flag_whole", ignorenulls=True).over(w7).alias("first_known_last3_min_new_group_flag_whole"),
        F.rank().over(w8).alias("last3_min_new_group_flag_rank"),
        F.count("last3_min_new_group_flag_whole").over(w9).alias("last3_min_new_group_flag_whole_count_7d"),
    )
)

enriched = (
    metrics_transactions
    .select(
        "*",
        F.lead("row_seq", 1).over(w10).alias("next_row_seq"),
        F.percent_rank().over(w11).alias("last3_min_new_group_flag_whole_bucket_percent_rank"),
        F.row_number().over(w12).alias("seq_no"),
    )
)

daily = (
    enriched
    .groupBy(
        F.when(F.col("last3_min_new_group_flag_whole_count_7d") >= 16, "HIGH").otherwise("LOW").alias("last3_min_new_group_flag_whole_count_7d_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("row_seq").alias("total_row_seq"),
        F.percentile_approx("last3_min_new_group_flag_whole", 0.5).alias("last3_min_new_group_flag_whole_p50"),
        F.count("*").alias("row_count"),
    )
)

result = (
    daily
    .select(
        "last3_min_new_group_flag_whole_count_7d_tier",
        "total_row_seq",
        "last3_min_new_group_flag_whole_p50",
    )
    .orderBy(F.col("total_row_seq").desc())
)
