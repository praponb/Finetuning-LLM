from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee_rounded", "id")
w2 = Window.partitionBy("customer_id", "status").orderBy("start_date", "id")
w_hist = Window.partitionBy("plan").orderBy("end_date_year", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy("last7_sum_prev_monthly_fee", "id")
w5 = Window.orderBy("last7_min_prev_monthly_fee", "id")
w6 = Window.orderBy("last7_min_prev_monthly_fee", "id").rowsBetween(-6, -1)
w7 = Window.orderBy("last7_count_prev_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-07-01").cast("date"))
    .select("*", F.round("monthly_fee", 0).alias("monthly_fee_rounded"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.ntile(10).over(w).alias("monthly_fee_rounded_bucket"),
        F.lag("monthly_fee", 2).over(w2).alias("prev_monthly_fee"),
    )
)

valid = (
    with_windows
    .filter((F.col("start_date") >= F.to_date(F.lit("2023-03-30"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "monthly_fee",
        "monthly_fee_rounded_bucket",
        "prev_monthly_fee",
        F.year("end_date").alias("end_date_year"),
        F.trim("plan").alias("clean_plan"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "monthly_fee",
        F.count("prev_monthly_fee").over(w_hist).alias("last7_count_prev_monthly_fee"),
        F.sum("prev_monthly_fee").over(w_hist).alias("last7_sum_prev_monthly_fee"),
        F.min("prev_monthly_fee").over(w_hist).alias("last7_min_prev_monthly_fee"),
    )
)

metrics_subscriptions = (
    metrics
    .select(
        "id",
        "monthly_fee",
        "last7_count_prev_monthly_fee",
        "last7_min_prev_monthly_fee",
        F.row_number().over(w4).alias("seq_no"),
        F.lag("monthly_fee", 2).over(w5).alias("prev_monthly_fee"),
        F.avg("last7_count_prev_monthly_fee").over(w6).alias("last7_count_prev_monthly_fee_avg_6_prior"),
        F.first("last7_sum_prev_monthly_fee", ignorenulls=True).over(w7).alias("first_known_last7_sum_prev_monthly_fee"),
        F.sum("last7_sum_prev_monthly_fee").over(w7).alias("running_last7_sum_prev_monthly_fee"),
    )
)

result = (
    metrics_subscriptions
    .groupBy("seq_no")
    .agg(
        F.round(F.avg("running_last7_sum_prev_monthly_fee"), 2).alias("avg_running_last7_sum_prev_monthly_fee"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("distinct_ids")
)
