from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status").orderBy("end_date", "id")
w2 = Window.orderBy("monthly_fee", "id")
w3 = Window.partitionBy("status").orderBy("prev_days_end_date_to_start_date_2", "id")
w4 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("days_end_date_to_start_date", "id")
w6 = Window.orderBy("days_end_date_to_start_date", "id").rowsBetween(-6, -1)
w7 = Window.orderBy(F.col("prev_days_end_date_to_start_date").asc_nulls_first())
w8 = Window.orderBy(F.col("prev_prev_days_end_date_to_start_date").asc_nulls_last())

filtered = (
    df_subscriptions
    .filter(F.col("end_date") > "2023-03-01")
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.date_trunc("WEEK", "start_date").alias("start_date_period"),
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
    )
)

scored = (
    filtered
    .filter(F.col("start_date_period") >= "2023-08-02")
    .select(
        "*",
        F.lag("days_end_date_to_start_date").over(w_grp).alias("prev_days_end_date_to_start_date"),
        F.lead("days_end_date_to_start_date").over(w_grp).alias("next_days_end_date_to_start_date"),
        F.lag("monthly_fee").over(w_grp).alias("prev_monthly_fee"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.lag("days_end_date_to_start_date", 2).over(w2).alias("prev_days_end_date_to_start_date_2"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "start_date",
        "start_date_period",
        "days_end_date_to_start_date",
        "prev_days_end_date_to_start_date",
        "prev_monthly_fee",
        "seq_no",
        F.lag("prev_days_end_date_to_start_date", 1).over(w3).alias("prev_prev_days_end_date_to_start_date"),
        F.sum("days_end_date_to_start_date").over(w4).alias("running_days_end_date_to_start_date"),
        F.ntile(10).over(w5).alias("days_end_date_to_start_date_bucket"),
    )
)

scored_subscriptions = (
    enriched
    .select(
        "id",
        "start_date_period",
        "days_end_date_to_start_date",
        "prev_days_end_date_to_start_date",
        "seq_no",
        "days_end_date_to_start_date_bucket",
        F.avg("running_days_end_date_to_start_date").over(w6).alias("running_days_end_date_to_start_date_avg_6_prior"),
        F.dense_rank().over(w7).alias("prev_days_end_date_to_start_date_dense_rank"),
        F.dense_rank().over(w8).alias("prev_prev_days_end_date_to_start_date_dense_rank"),
    )
)

cleaned = (
    scored_subscriptions
    .filter(F.col("prev_days_end_date_to_start_date_dense_rank") >= 6)
    .select(
        "id",
        "days_end_date_to_start_date",
        "running_days_end_date_to_start_date_avg_6_prior",
        "prev_days_end_date_to_start_date_dense_rank",
        "prev_prev_days_end_date_to_start_date_dense_rank",
        (F.col("seq_no") + F.col("days_end_date_to_start_date")).alias("seq_no_plus_days_end_date_to_start_date"),
        (F.col("seq_no") + F.col("prev_prev_days_end_date_to_start_date_dense_rank")).alias("seq_no_plus_prev_prev_days_end_date_to_start_date_dense_rank"),
    )
)

daily = (
    cleaned
    .cube(
        F.when(F.col("prev_prev_days_end_date_to_start_date_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("prev_prev_days_end_date_to_start_date_dense_rank_tier"),
        F.when(F.col("running_days_end_date_to_start_date_avg_6_prior") >= 308, "LARGE").otherwise("SMALL").alias("running_days_end_date_to_start_date_avg_6_prior_tier"),
    )
    .agg(
        F.percentile_approx("prev_days_end_date_to_start_date_dense_rank", 0.5).alias("prev_days_end_date_to_start_date_dense_rank_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_days_end_date_to_start_date_avg_6_prior", 0.95).alias("running_days_end_date_to_start_date_avg_6_prior_p95"),
    )
)

result = (
    daily
    .select(
        "prev_prev_days_end_date_to_start_date_dense_rank_tier",
        "running_days_end_date_to_start_date_avg_6_prior_tier",
        "distinct_ids",
        "running_days_end_date_to_start_date_avg_6_prior_p95",
    )
    .orderBy(
        F.col("prev_prev_days_end_date_to_start_date_dense_rank_tier").desc_nulls_first(),
    )
)
