from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy(F.col("last3_count_amount").desc_nulls_last())
w3 = Window.partitionBy("txn_type").orderBy(F.col("last3_sum_amount").asc_nulls_last(), "id")
w4 = Window.partitionBy("merchant").orderBy(F.col("last3_avg_amount").desc_nulls_first(), "id")
w5 = Window.partitionBy("last3_sum_amount_tier").orderBy(F.col("total_last3_sum_amount_whole").desc())

with_windows = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "merchant",
        F.count("amount").over(w).alias("last3_count_amount"),
        F.sum("amount").over(w).alias("last3_sum_amount"),
        F.avg("amount").over(w).alias("last3_avg_amount"),
    )
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

deduped_transactions = (
    deduped
    .withColumn("dedup_rank_2", F.row_number().over(w3))
    .filter(F.col("dedup_rank_2") == 1)
    .select("id", "merchant", "last3_sum_amount", "last3_avg_amount")
)

ranked = (
    deduped_transactions
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

filtered = (
    ranked
    .filter(F.col("last3_sum_amount") > 9000)
    .select(
        "id",
        "merchant",
        "last3_sum_amount",
        F.least("last3_avg_amount", "rank_in_group").alias("least_last3_avg_amount_rank_in_group"),
        F.ceil("last3_sum_amount").alias("last3_sum_amount_whole"),
    )
)

stats = (
    filtered
    .groupBy(
        F.when(F.col("last3_sum_amount") >= 20400, "HIGH").otherwise("LOW").alias("last3_sum_amount_tier"),
        "merchant",
    )
    .agg(
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_rows"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.sum("last3_sum_amount_whole").alias("total_last3_sum_amount_whole"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"),
    )
    .withColumn("total_last3_sum_amount_whole_rank", F.rank().over(w5))
)

result = (
    stats
    .select(
        "last3_sum_amount_tier",
        "merchant",
        "shell_rows",
        "num_merchants",
        "shell_count",
        "total_last3_sum_amount_whole_rank",
    )
    .orderBy("total_last3_sum_amount_whole_rank")
)
