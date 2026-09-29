from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id")
w2 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant", "txn_type").orderBy("row_seq", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("merchant")
w5 = Window.partitionBy("txn_type").orderBy(F.col("prev_amount").desc())
w6 = Window.partitionBy("merchant").orderBy("row_seq", "id")
w7 = Window.partitionBy("merchant").orderBy(F.col("row_seq").desc(), "id")
w8 = Window.partitionBy("merchant").orderBy(F.col("prev_prev_amount_share").desc(), "id")
w9 = Window.partitionBy("merchant").orderBy(F.col("prev_prev_amount_share").desc_nulls_last())

scored = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged",
        F.lag("amount").over(w_part).alias("prev_amount"),
        F.row_number().over(w_part).alias("row_seq"),
        F.last("merchant", ignorenulls=True).over(w2).alias("last_known_merchant"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.avg("prev_amount").over(w3).alias("prev_amount_avg_6"),
        F.round((F.col("prev_amount") / F.sum("prev_amount").over(w4)), 4).alias("prev_amount_share"),
        F.rank().over(w5).alias("prev_amount_rank"),
    )
)

scored_transactions = (
    with_windows
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "merchant",
        "prev_amount",
        "prev_amount_share",
        "prev_amount_rank",
        F.lag("prev_amount_rank").over(w6).alias("prev_prev_amount_rank"),
        F.lead("prev_amount_share").over(w6).alias("next_prev_amount_share"),
        F.lag("prev_amount_share").over(w6).alias("prev_prev_amount_share"),
        F.row_number().over(w6).alias("row_seq_2"),
        F.ntile(10).over(w7).alias("row_seq_bucket"),
    )
)

latest = (
    scored_transactions
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

kept_keys = (
    latest
    .select("id", "merchant")
    .subtract(
        df_transactions
        .filter(F.to_date("txn_time") < "2024-01-25")
        .select("id", "merchant")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

with_windows_transactions = (
    reconciled
    .select(
        "id",
        "prev_amount",
        "prev_amount_share",
        "prev_prev_amount_share",
        "row_seq_bucket",
        F.dense_rank().over(w9).alias("prev_prev_amount_share_dense_rank"),
        F.round((F.col("prev_prev_amount_rank") / F.sum("prev_prev_amount_rank").over(w4)), 4).alias("prev_prev_amount_rank_share"),
    )
)

result = (
    with_windows_transactions
    .groupBy(
        F.when(F.col("prev_prev_amount_share") >= 0.64, "HIGH").otherwise("LOW").alias("prev_prev_amount_share_tier"),
        F.when(F.col("prev_prev_amount_rank_share") >= 0.65, "PREMIUM").otherwise("STANDARD").alias("prev_prev_amount_rank_share_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.max("prev_prev_amount_share_dense_rank").alias("max_prev_prev_amount_share_dense_rank"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy(F.col("prev_prev_amount_rank_share_tier").desc_nulls_last())
)
