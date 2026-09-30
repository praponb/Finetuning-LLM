from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("start_date").asc_nulls_last())
w3 = Window.orderBy("days_start_date_to_end_date", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("days_start_date_to_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("days_start_date_to_end_date").desc_nulls_first())
w6 = Window.orderBy("days_start_date_to_end_date_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("days_start_date_to_end_date_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.orderBy("days_start_date_to_end_date_avg_2", "id").rowsBetween(-3, -1)
w10 = Window.orderBy("cumulative_days_start_date_to_end_date", "id").rowsBetween(-5, -1)
w_running = Window.orderBy("days_start_date_to_end_date_avg_2", "id").rowsBetween(-6, -1)
w_ordered = Window.orderBy("running_min_days_start_date_to_end_date_avg_2", "id")
w_ordered_2 = Window.orderBy("first_known_days_start_date_to_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-04-12").cast("date")) & (F.col("status") == "EXPIRED"))
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped_subscriptions = (
    deduped
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "end_date", "days_start_date_to_end_date")
)

metrics = (
    deduped_subscriptions
    .withColumn("days_start_date_to_end_date_avg_2", F.avg("days_start_date_to_end_date").over(w3))
    .withColumn("cumulative_days_start_date_to_end_date", F.sum("days_start_date_to_end_date").over(w4))
    .withColumn("days_start_date_to_end_date_rank", F.rank().over(w5))
    .withColumn("days_start_date_to_end_date_cume_dist", F.cume_dist().over(w5))
)

with_windows = (
    metrics
    .filter(F.col("days_start_date_to_end_date") == 58)
    .select(
        "id",
        "end_date",
        "days_start_date_to_end_date_avg_2",
        "days_start_date_to_end_date_rank",
        F.sum("days_start_date_to_end_date").over(w6).alias("running_days_start_date_to_end_date"),
        F.first("days_start_date_to_end_date", ignorenulls=True).over(w7).alias("first_known_days_start_date_to_end_date"),
        F.avg("days_start_date_to_end_date").over(w8).alias("days_start_date_to_end_date_avg_2_2"),
        F.avg("cumulative_days_start_date_to_end_date").over(w9).alias("cumulative_days_start_date_to_end_date_avg_3_prior"),
        F.avg("days_start_date_to_end_date").over(w10).alias("days_start_date_to_end_date_avg_5_prior"),
    )
)

with_windows_subscriptions = (
    with_windows
    .select(
        "id",
        "end_date",
        "days_start_date_to_end_date_avg_2",
        "running_days_start_date_to_end_date",
        "first_known_days_start_date_to_end_date",
        "cumulative_days_start_date_to_end_date_avg_3_prior",
        "days_start_date_to_end_date_avg_5_prior",
        F.avg("days_start_date_to_end_date_avg_2").over(w7).alias("running_avg_days_start_date_to_end_date_avg_2"),
        F.min("days_start_date_to_end_date_avg_2").over(w7).alias("running_min_days_start_date_to_end_date_avg_2"),
        F.count("days_start_date_to_end_date_avg_2").over(w7).alias("running_count_days_start_date_to_end_date_avg_2"),
    )
)

scored = (
    with_windows_subscriptions
    .withColumn("running_avg_days_start_date_to_end_date_avg_2_avg_6_prior", F.avg("running_avg_days_start_date_to_end_date_avg_2").over(w_running))
    .withColumn("first_known_running_avg_days_start_date_to_end_date_avg_2", F.first("running_avg_days_start_date_to_end_date_avg_2", ignorenulls=True).over(w6))
    .withColumn("running_min_days_start_date_to_end_date_avg_2_bucket", F.ntile(5).over(w_ordered))
    .withColumn("last_known_running_days_start_date_to_end_date", F.last("running_days_start_date_to_end_date", ignorenulls=True).over(w_ordered_2))
)

daily = (
    scored
    .groupBy(
        F.date_trunc("week", "end_date").alias("week_start"),
        F.when(F.col("cumulative_days_start_date_to_end_date_avg_3_prior") >= 400, "LARGE").otherwise("SMALL").alias("cumulative_days_start_date_to_end_date_avg_3_prior_tier"),
    )
    .agg(
        F.round(F.avg("first_known_running_avg_days_start_date_to_end_date_avg_2"), 2).alias("avg_first_known_running_avg_days_start_date_to_end_date_avg_2"),
        F.percentile_approx("running_avg_days_start_date_to_end_date_avg_2", 0.5).alias("running_avg_days_start_date_to_end_date_avg_2_p50"),
    )
)

result = (
    daily
    .select(
        "week_start",
        "cumulative_days_start_date_to_end_date_avg_3_prior_tier",
        "avg_first_known_running_avg_days_start_date_to_end_date_avg_2",
        "running_avg_days_start_date_to_end_date_avg_2_p50",
    )
    .orderBy("running_avg_days_start_date_to_end_date_avg_2_p50")
)
