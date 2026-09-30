from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")
w4 = Window.partitionBy("merchant", "txn_type").orderBy("txn_time", "id")
w5 = Window.partitionBy("account_id", "txn_type").orderBy("amount", "id")
w6 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("prev_new_group_flag").asc_nulls_last(), "id")

flagged = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

deduped = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    deduped
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        F.lag("burst_id").over(w4).alias("prev_burst_id"),
        F.lead("burst_id").over(w4).alias("next_burst_id"),
        F.lead("new_group_flag").over(w4).alias("next_new_group_flag"),
        F.lag("new_group_flag").over(w4).alias("prev_new_group_flag"),
        F.row_number().over(w4).alias("seq_no"),
        F.lead("burst_id", 2).over(w5).alias("next_burst_id_2"),
    )
)

deduped_transactions = (
    enriched
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    deduped_transactions
    .filter(F.col("txn_time") > F.lit("2023-06-07").cast("date"))
    .select(
        "id",
        "txn_type",
        "merchant",
        "next_burst_id",
        "next_new_group_flag",
        "prev_new_group_flag",
        "seq_no",
        "next_burst_id_2",
        F.greatest("next_burst_id", "seq_no").alias("greatest_next_burst_id_seq_no"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("seq_no") >= 8, "PREMIUM").otherwise("STANDARD").alias("seq_no_tier"),
        "txn_type",
    )
    .agg(
        F.max(F.when(F.col("merchant") == "Grab", F.col("seq_no"))).alias("grab_max_seq_no"),
        F.countDistinct("txn_type").alias("distinct_txn_types"),
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("prev_new_group_flag")).otherwise(0)).alias("refund_prev_new_group_flag"),
        F.percentile_approx("prev_new_group_flag", 0.9).alias("prev_new_group_flag_p90"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
    )
    .orderBy(F.col("prev_new_group_flag_p90").desc_nulls_first())
)
