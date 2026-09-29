from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("week_start").orderBy(F.col("num_plans").desc())

filtered = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-09-15").cast("date"))
    .withColumn("start_date_plus_months", F.add_months("start_date", 3))
    .withColumn("days_since_end_date", F.datediff(F.current_date(), "end_date"))
)

kept_keys = (
    filtered
    .select("status", "id")
    .subtract(
        df_subscriptions
        .filter(F.year("end_date") == 2025)
        .select("status", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered_subscriptions = (
    verified
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-09-05")))
    .withColumn("days_since_end_date_times_monthly_fee", (F.col("days_since_end_date") * F.col("monthly_fee")))
    .withColumn("start_date_quarter", F.quarter("start_date"))
)

scored = (
    filtered_subscriptions
    .filter(F.col("plan") == "standard")
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "start_date_plus_months",
        "days_since_end_date",
        "days_since_end_date_times_monthly_fee",
        F.stddev_samp("monthly_fee").over(w).alias("status_stddev_samp_monthly_fee"),
    )
)

totals = (
    scored
    .groupBy(
        F.date_trunc("WEEK", "start_date_plus_months").alias("week_start"),
        "customer_id",
    )
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.sum(F.when(F.col("plan") == "basic", F.col("status_stddev_samp_monthly_fee"))).alias("basic_total_status_stddev_samp_monthly_fee"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("monthly_fee_per_basic"),
    )
    .withColumn("num_plans_percent_rank", F.percent_rank().over(w2))
)

result = (
    totals
    .select(
        "week_start",
        "customer_id",
        "num_plans",
        "basic_total_status_stddev_samp_monthly_fee",
        "monthly_fee_per_basic",
        "num_plans_percent_rank",
    )
    .orderBy(F.col("num_plans").desc(), "num_plans_percent_rank")
)
