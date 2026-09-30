from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status", "customer_id").orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("status").orderBy(F.col("last4_min_monthly_fee").asc_nulls_first())
w4 = Window.partitionBy("status").orderBy("last4_min_monthly_fee", "id").rowsBetween(-6, -1)
w_hist = Window.partitionBy("status").orderBy("last4_count_monthly_fee", "id")
w_grp = Window.partitionBy("status").orderBy("last4_count_monthly_fee_avg_6_prior", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("status").orderBy("last4_count_monthly_fee_avg_6_prior", "id")
w8 = Window.partitionBy("status").orderBy(F.col("last3_max_last4_count_monthly_fee_avg_6_prior").desc_nulls_first(), "id")
w9 = Window.partitionBy("status")
w10 = Window.orderBy(F.col("last3_max_last4_count_monthly_fee_avg_6_prior").desc(), "id")
w11 = Window.partitionBy("status").orderBy("last4_min_monthly_fee_rank", "id").rowsBetween(-5, -1)
w12 = Window.partitionBy("status").orderBy("last3_avg_last4_count_monthly_fee_avg_6_prior", "id")
w13 = Window.partitionBy("status").orderBy("last4_count_monthly_fee_avg_6_prior_avg_5_prior", "id").rowsBetween(-2, Window.currentRow)
w14 = Window.partitionBy("status").orderBy("last3_sum_last4_count_monthly_fee_avg_6_prior", "id")
w15 = Window.partitionBy("status").orderBy(F.col("last3_sum_last4_count_monthly_fee_avg_6_prior").desc(), "id")

base = (
    df_subscriptions
    .filter(F.col("start_date") >= F.to_date(F.lit("2023-05-18")))
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "status",
        F.lower("plan").alias("plan_lower"),
    )
)

scored = (
    base
    .select(
        "id",
        "monthly_fee",
        "status",
        F.sum("monthly_fee").over(w_running).alias("last4_sum_monthly_fee"),
        F.count("monthly_fee").over(w_running).alias("last4_count_monthly_fee"),
        F.min("monthly_fee").over(w_running).alias("last4_min_monthly_fee"),
        F.avg("monthly_fee").over(w_running).alias("last4_avg_monthly_fee"),
        F.ntile(3).over(w2).alias("monthly_fee_bucket"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "status",
        F.rank().over(w3).alias("last4_min_monthly_fee_rank"),
        F.avg("last4_count_monthly_fee").over(w4).alias("last4_count_monthly_fee_avg_6_prior"),
        F.lag("monthly_fee_bucket", 2, 0).over(w_hist).alias("prev_monthly_fee_bucket"),
    )
)

enriched_subscriptions = (
    enriched
    .withColumn("last3_max_last4_count_monthly_fee_avg_6_prior", F.max("last4_count_monthly_fee_avg_6_prior").over(w_grp))
    .withColumn("last3_sum_last4_count_monthly_fee_avg_6_prior", F.sum("last4_count_monthly_fee_avg_6_prior").over(w_grp))
    .withColumn("last3_min_last4_count_monthly_fee_avg_6_prior", F.min("last4_count_monthly_fee_avg_6_prior").over(w_grp))
    .withColumn("last3_avg_last4_count_monthly_fee_avg_6_prior", F.avg("last4_count_monthly_fee_avg_6_prior").over(w_grp))
    .withColumn("next_prev_monthly_fee_bucket", F.lead("prev_monthly_fee_bucket", 1).over(w_part))
)

deduped = (
    enriched_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    deduped
    .filter(F.col("last3_avg_last4_count_monthly_fee_avg_6_prior") >= 6.9)
    .select(
        "id",
        "status",
        "prev_monthly_fee_bucket",
        "last3_max_last4_count_monthly_fee_avg_6_prior",
        "last3_sum_last4_count_monthly_fee_avg_6_prior",
        "last3_avg_last4_count_monthly_fee_avg_6_prior",
        "rank_in_group",
        F.sum("prev_monthly_fee_bucket").over(w9).alias("status_sum_prev_monthly_fee_bucket"),
        F.ntile(5).over(w10).alias("last3_max_last4_count_monthly_fee_avg_6_prior_bucket"),
        F.avg("last4_count_monthly_fee_avg_6_prior").over(w11).alias("last4_count_monthly_fee_avg_6_prior_avg_5_prior"),
    )
)

enriched_subscriptions_2 = (
    metrics
    .select(
        "id",
        "prev_monthly_fee_bucket",
        "last3_sum_last4_count_monthly_fee_avg_6_prior",
        "last3_avg_last4_count_monthly_fee_avg_6_prior",
        "rank_in_group",
        "status_sum_prev_monthly_fee_bucket",
        "last3_max_last4_count_monthly_fee_avg_6_prior_bucket",
        F.lead("last3_max_last4_count_monthly_fee_avg_6_prior_bucket", 1, 0).over(w12).alias("next_last3_max_last4_count_monthly_fee_avg_6_prior_bucket"),
        F.avg("last3_max_last4_count_monthly_fee_avg_6_prior").over(w13).alias("last3_max_last4_count_monthly_fee_avg_6_prior_avg_2"),
        F.ntile(5).over(w14).alias("last3_sum_last4_count_monthly_fee_avg_6_prior_bucket"),
        F.row_number().over(w15).alias("event_seq"),
    )
)

result = (
    enriched_subscriptions_2
    .select(
        "id",
        "prev_monthly_fee_bucket",
        "last3_sum_last4_count_monthly_fee_avg_6_prior",
        "rank_in_group",
        "last3_max_last4_count_monthly_fee_avg_6_prior_bucket",
        "next_last3_max_last4_count_monthly_fee_avg_6_prior_bucket",
        "last3_sum_last4_count_monthly_fee_avg_6_prior_bucket",
    )
    .orderBy(F.col("prev_monthly_fee_bucket").desc())
)
