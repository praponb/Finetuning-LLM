from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id")
w3 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("running_monthly_fee").desc(), "id")
w_running = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("plan", "status")
w7 = Window.partitionBy("status", "plan").orderBy(F.col("first_known_monthly_fee").desc(), "id")
w8 = Window.orderBy(F.col("cumulative_monthly_fee").asc_nulls_last())

base = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-09-03").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("plan_status_key", F.concat_ws("-", "plan", "status"))
)

scored = (
    base
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status",
        "plan_status_key",
        F.first("monthly_fee", ignorenulls=True).over(w).alias("first_known_monthly_fee"),
        F.stddev_samp("monthly_fee").over(w2).alias("customer_id_stddev_samp_monthly_fee"),
        F.sum("monthly_fee").over(w3).alias("running_monthly_fee"),
    )
)

with_windows = (
    scored
    .filter(F.col("customer_id_stddev_samp_monthly_fee").between(76, 233))
    .select(
        "id",
        "monthly_fee",
        "plan_status_key",
        "first_known_monthly_fee",
        "running_monthly_fee",
        F.row_number().over(w_grp).alias("row_seq"),
        F.sum("first_known_monthly_fee").over(w_running).alias("running_first_known_monthly_fee"),
        F.sum("running_monthly_fee").over(w6).alias("plan_sum_running_monthly_fee"),
        F.sum("monthly_fee").over(w_running).alias("cumulative_monthly_fee"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

filtered = (
    with_windows
    .filter(F.col("cumulative_monthly_fee") <= 435)
    .withColumn("plan_sum_running_monthly_fee_str", F.col("plan_sum_running_monthly_fee").cast("double"))
)

matched_keys = (
    filtered
    .select("id")
    .subtract(
        df_subscriptions
        .filter(F.month("start_date").isin([5, 6, 11]))
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "monthly_fee",
        "first_known_monthly_fee",
        "row_seq",
        "running_first_known_monthly_fee",
        "plan_sum_running_monthly_fee",
        "cumulative_monthly_fee",
        F.percent_rank().over(w8).alias("cumulative_monthly_fee_percent_rank"),
    )
)
