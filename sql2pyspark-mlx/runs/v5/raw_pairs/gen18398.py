from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("amount").desc(), "id")
w3 = Window.orderBy(F.col("amount").desc_nulls_first())
w4 = Window.partitionBy("txn_type")
w5 = Window.partitionBy("merchant").orderBy(F.col("amount_share").desc())
w6 = Window.partitionBy("merchant").orderBy("amount_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("merchant")
w8 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w9 = Window.partitionBy("merchant").orderBy(F.col("amount_share_percent_rank").desc(), "id")
w10 = Window.partitionBy("merchant").orderBy(F.col("amount_bucket_share").desc_nulls_last(), "id")
w11 = Window.orderBy("txn_time", "id")
w12 = Window.orderBy("amount_bucket", "id")
w13 = Window.orderBy("amount_share_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.orderBy("amount_bucket", "id").rowsBetween(-2, -1)
w15 = Window.orderBy("amount_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w16 = Window.orderBy("amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w17 = Window.orderBy("seq_no", "id").rowsBetween(-5, -1)

enriched = (
    df_transactions
    .filter(F.col("amount") > 2300)
    .select(
        "id",
        "txn_time",
        "merchant",
        F.first("merchant", ignorenulls=True).over(w).alias("first_known_merchant"),
        F.ntile(5).over(w2).alias("amount_bucket"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
    )
)

enriched_transactions = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w5).alias("amount_share_percent_rank"),
        F.last("amount_cume_dist", ignorenulls=True).over(w6).alias("last_known_amount_cume_dist"),
    )
)

scored = (
    enriched_transactions
    .withColumn("amount_bucket_share", F.round((F.col("amount_bucket") / F.sum("amount_bucket").over(w7)), 4))
    .withColumn("amount_share_percent_rank_sum_30d", F.sum("amount_share_percent_rank").over(w8))
    .withColumn("seq_no", F.row_number().over(w9))
    .withColumn("merchant_max_amount_bucket", F.max("amount_bucket").over(w7))
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w10))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "txn_time",
        "first_known_merchant",
        "amount_bucket",
        "amount_cume_dist",
        "amount_share",
        "amount_share_percent_rank",
        "last_known_amount_cume_dist",
        "seq_no",
    )
)

with_windows = (
    ranked
    .withColumn("next_amount_cume_dist", F.lead("amount_cume_dist", 1).over(w11))
    .withColumn("amount_bucket_bucket", F.ntile(3).over(w12))
    .withColumn("first_known_amount_share_percent_rank", F.first("amount_share_percent_rank", ignorenulls=True).over(w13))
    .withColumn("seq_no_avg_2_prior", F.avg("seq_no").over(w14))
)

result = (
    with_windows
    .select(
        "id",
        "first_known_merchant",
        "amount_bucket",
        "amount_share_percent_rank",
        "last_known_amount_cume_dist",
        "next_amount_cume_dist",
        "first_known_amount_share_percent_rank",
        "seq_no_avg_2_prior",
        F.last("amount_share", ignorenulls=True).over(w15).alias("last_known_amount_share"),
        F.last("first_known_amount_share_percent_rank", ignorenulls=True).over(w16).alias("last_known_first_known_amount_share_percent_rank"),
        F.avg("seq_no_avg_2_prior").over(w17).alias("seq_no_avg_2_prior_avg_5_prior"),
    )
)
