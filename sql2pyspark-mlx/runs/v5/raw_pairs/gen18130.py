from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_last())
w_ordered = Window.orderBy("amount_dense_rank", "id")
w4 = Window.partitionBy("merchant").orderBy("next_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w_grp = Window.partitionBy("merchant").orderBy("first_known_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("txn_type", "merchant").orderBy("row_seq_2", "id")

enriched = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
    )
)

enriched_transactions = (
    enriched
    .filter(F.col("amount_dense_rank") < 7)
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w4))
    .withColumn("row_seq_2", F.row_number().over(w5))
)

metrics = (
    enriched_transactions
    .select(
        "id",
        "amount",
        "merchant",
        "row_seq",
        "first_known_amount",
        F.max("row_seq").over(w_grp).alias("running_max_row_seq"),
        F.count("row_seq").over(w_grp).alias("running_count_row_seq"),
        F.sum("row_seq").over(w_grp).alias("running_sum_row_seq"),
        F.lead("amount_dense_rank", 1).over(w7).alias("next_amount_dense_rank"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "amount",
        "first_known_amount",
        "running_max_row_seq",
        "running_sum_row_seq",
        "next_amount_dense_rank",
    )
)

base = (
    ranked
    .filter(F.col("next_amount_dense_rank") <= 5)
    .select(
        "id",
        "amount",
        "running_max_row_seq",
        "running_sum_row_seq",
        "next_amount_dense_rank",
        F.col("running_max_row_seq").cast("int").alias("running_max_row_seq_int"),
    )
)

grouped = (
    base
    .groupBy(
        F.when(F.col("running_max_row_seq") >= 9.0, "PREMIUM").otherwise("STANDARD").alias("running_max_row_seq_tier"),
        F.when(F.col("running_sum_row_seq") >= 34, "PREMIUM").otherwise("STANDARD").alias("running_sum_row_seq_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("running_max_row_seq").alias("max_running_max_row_seq"),
        F.round(F.avg("running_sum_row_seq"), 2).alias("avg_running_sum_row_seq"),
    )
    .filter(F.col("distinct_ids") > 1)
)

result = (
    grouped
    .select(
        "running_max_row_seq_tier",
        "running_sum_row_seq_tier",
        "max_running_max_row_seq",
        "avg_running_sum_row_seq",
    )
    .orderBy(F.col("max_running_max_row_seq").desc_nulls_first())
)
