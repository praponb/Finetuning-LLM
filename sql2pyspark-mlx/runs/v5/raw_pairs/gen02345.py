from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type")
w4 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w5 = Window.partitionBy("merchant")
w6 = Window.partitionBy("txn_type").orderBy(F.col("cumulative_amount_share").desc_nulls_first(), "id")
w7 = Window.partitionBy("txn_type").orderBy(F.col("txn_type_count_amount").asc_nulls_first(), "id")
w8 = Window.partitionBy("txn_type").orderBy(F.col("rank_in_group_per_txn_type_count_amount_share").desc())
w_running = Window.orderBy("txn_type_count_amount_share_per_cumulative_amount", "id").rowsBetween(-5, -1)
w10 = Window.orderBy("rank_in_group", "id")

scored = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "merchant",
        F.first("amount", ignorenulls=True).over(w).alias("first_known_amount"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
        F.count("amount").over(w3).alias("txn_type_count_amount"),
        F.lead("amount", 1).over(w4).alias("next_amount"),
    )
)

enriched = (
    scored
    .filter(F.col("merchant") != "Shell")
    .select(
        "id",
        "txn_type",
        "cumulative_amount",
        "txn_type_count_amount",
        F.round((F.col("txn_type_count_amount") / F.sum("txn_type_count_amount").over(w5)), 4).alias("txn_type_count_amount_share"),
        F.round((F.col("cumulative_amount") / F.sum("cumulative_amount").over(w5)), 4).alias("cumulative_amount_share"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    deduped
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

valid = (
    ranked
    .filter(F.col("rank_in_group") > 1)
    .select(
        "id",
        "txn_type",
        "cumulative_amount",
        "cumulative_amount_share",
        "rank_in_group",
        F.round((F.col("txn_type_count_amount_share") / F.col("cumulative_amount")), 2).alias("txn_type_count_amount_share_per_cumulative_amount"),
        (F.col("rank_in_group") / F.nullif("txn_type_count_amount_share", F.lit(0))).alias("rank_in_group_per_txn_type_count_amount_share"),
    )
)

scored_transactions = (
    valid
    .select(
        "id",
        "txn_type",
        "txn_type_count_amount_share_per_cumulative_amount",
        "rank_in_group_per_txn_type_count_amount_share",
        F.rank().over(w8).alias("rank_in_group_per_txn_type_count_amount_share_rank"),
        F.avg("cumulative_amount_share").over(w_running).alias("cumulative_amount_share_avg_5_prior"),
        F.lead("rank_in_group_per_txn_type_count_amount_share", 1).over(w10).alias("next_rank_in_group_per_txn_type_count_amount_share"),
    )
)

result = (
    scored_transactions
    .groupBy("txn_type")
    .agg(
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_rows"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_count"),
        F.percentile_approx("rank_in_group_per_txn_type_count_amount_share", [0.25, 0.5, 0.75]).alias("rank_in_group_per_txn_type_count_amount_share_quartiles"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
    )
    .orderBy(F.col("num_txn_types").desc_nulls_first())
)
