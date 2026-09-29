from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status", "plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id")
w_recent = Window.partitionBy("status").orderBy("is_expired", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("plan", "status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-03-05").cast("date"))
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.substring("plan", 1, 3).alias("plan_prefix"),
    )
)

valid_subscriptions = (
    valid
    .filter(F.col("plan") == "enterprise")
    .select(
        "*",
        F.when(F.col("status") == "EXPIRED", 1).otherwise(0).alias("is_expired"),
    )
)

with_windows = (
    valid_subscriptions
    .withColumn("running_avg_is_expired", F.avg("is_expired").over(w_ordered))
    .withColumn("running_max_is_expired", F.max("is_expired").over(w_ordered))
    .withColumn("running_sum_is_expired", F.sum("is_expired").over(w_ordered))
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 1).over(w2))
)

with_windows_subscriptions = (
    with_windows
    .withColumn("running_min_prev_monthly_fee", F.min("prev_monthly_fee").over(w_recent))
    .withColumn("running_avg_prev_monthly_fee", F.avg("prev_monthly_fee").over(w_recent))
    .withColumn("running_count_prev_monthly_fee", F.count("prev_monthly_fee").over(w_recent))
    .withColumn("cumulative_is_expired", F.sum("is_expired").over(w4))
)

result = (
    with_windows_subscriptions
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "plan_prefix",
        "is_expired",
        "running_avg_is_expired",
        "running_sum_is_expired",
        "prev_monthly_fee",
        "running_min_prev_monthly_fee",
    )
    .orderBy(F.col("monthly_fee").desc(), "is_expired")
)
