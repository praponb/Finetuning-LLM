from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-6, -1)
w_recent = Window.partitionBy("status").orderBy(F.col("amount_whole").desc(), "id")
w_hist = Window.orderBy("amount_whole", "id")
w5 = Window.orderBy("amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("status").orderBy("amount_whole_bucket_2", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy("status").orderBy("amount_whole_bucket_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("status").orderBy("running_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_part = Window.orderBy(F.col("running_amount_whole_avg_6_prior").desc_nulls_last())
w12 = Window.orderBy("amount_whole_bucket", "id")
w13 = Window.orderBy(F.col("running_amount_whole_avg_6_prior_rank").desc(), "id")
w14 = Window.partitionBy("status").orderBy(F.col("amount_whole_avg_6_prior").asc_nulls_last())

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.to_date(F.lit("2023-06-06"))) & F.col("order_id").isNotNull() & (F.col("paid_at") >= "2025-10-27"))
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        F.ceil("amount").alias("amount_whole"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.ntile(5).over(w).alias("amount_bucket"),
        F.avg("amount_whole").over(w2).alias("amount_whole_avg_6_prior"),
        F.ntile(4).over(w_recent).alias("amount_whole_bucket"),
        F.ntile(5).over(w_hist).alias("amount_whole_bucket_2"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.sum("amount_whole_avg_6_prior").over(w5).alias("running_amount_whole_avg_6_prior"),
        F.min("amount_whole_avg_6_prior").over(w6).alias("status_min_amount_whole_avg_6_prior"),
        F.avg("amount_whole").over(w7).alias("amount_whole_avg_2_prior"),
        F.sum("amount").over(w8).alias("running_amount"),
    )
)

enriched = (
    scored
    .withColumn("last_known_running_amount", F.last("running_amount", ignorenulls=True).over(w9))
    .withColumn("status_min_amount_whole_avg_6_prior_sum_90d", F.sum("status_min_amount_whole_avg_6_prior").over(w10))
    .withColumn("running_amount_whole_avg_6_prior_rank", F.rank().over(w_part))
    .withColumn("amount_whole_bucket_2_share", F.round((F.col("amount_whole_bucket_2") / F.sum("amount_whole_bucket_2").over(w6)), 4))
)

enriched_payments = (
    enriched
    .select(
        "*",
        F.lag("amount_bucket", 1).over(w12).alias("prev_amount_bucket"),
        F.ntile(4).over(w13).alias("running_amount_whole_avg_6_prior_rank_bucket"),
        F.sum("status_min_amount_whole_avg_6_prior_sum_90d").over(w6).alias("status_sum_status_min_amount_whole_avg_6_prior_sum_90d"),
        F.rank().over(w14).alias("amount_whole_avg_6_prior_rank"),
    )
)

result = (
    enriched_payments
    .select(
        "id",
        "paid_at",
        "status",
        "method_clean",
        "amount_bucket",
        "amount_whole_bucket",
        "amount_whole_bucket_2",
        "status_min_amount_whole_avg_6_prior_sum_90d",
        "running_amount_whole_avg_6_prior_rank",
        F.when(F.col("status").rlike("^(FAILED|SUCCESS)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("status_zone"),
    )
)
