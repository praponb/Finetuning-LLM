from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time_hour").desc(), "id")
w_ordered = Window.partitionBy("txn_type").orderBy("txn_time_hour", "id")
w_ordered_2 = Window.partitionBy("account_id", "merchant").orderBy("txn_time_hour", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy(F.col("txn_time_hour").desc())
w5 = Window.orderBy(F.col("amount_avg_3").desc())
w6 = Window.partitionBy("merchant").orderBy(F.col("amount_avg_3_dense_rank").desc_nulls_first(), "id")

filtered = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2024-02-09"))) & F.col("account_id").isin([9, 18]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.hour("txn_time").alias("txn_time_hour"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    deduped
    .select(
        "*",
        F.ntile(10).over(w_ordered).alias("txn_time_hour_bucket"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.avg("amount").over(w_ordered_2).alias("amount_avg_3"),
    )
)

valid = (
    enriched
    .filter(F.col("txn_time_hour_bucket").between(6, 8))
    .select(
        "*",
        F.round((F.col("amount_avg_3") / F.col("row_seq")), 2).alias("amount_avg_3_per_row_seq"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

enriched_transactions = (
    valid
    .withColumn("txn_time_hour_cume_dist", F.cume_dist().over(w4))
    .withColumn("amount_avg_3_dense_rank", F.dense_rank().over(w5))
)

latest = (
    enriched_transactions
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .select(
        "id",
        "account_id",
        "amount",
        "is_flagged",
        "txn_time_hour",
        "txn_time_hour_bucket",
        "amount_avg_3",
        "txn_time_hour_cume_dist",
        "rank_in_group",
    )
    .orderBy(F.col("rank_in_group").desc_nulls_last(), "amount_avg_3")
)
