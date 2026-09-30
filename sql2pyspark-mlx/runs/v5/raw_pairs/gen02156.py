from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "plan").orderBy("end_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_first())
w4 = Window.partitionBy("plan").orderBy(F.col("subscription_id").desc())
w5 = Window.partitionBy("customer_id").orderBy(F.col("days_since_end_date").desc(), "id", "subscription_id")
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("customer_id").orderBy("last_known_monthly_fee_scaled", "id", "subscription_id").rowsBetween(-6, Window.currentRow)
w8 = Window.orderBy(F.col("last_known_monthly_fee_2").desc(), "id", "subscription_id")

filtered = (
    df_subscriptions
    .filter((F.col("start_date") >= F.to_date(F.lit("2023-07-16"))) & F.col("start_date").between("2024-05-01", "2024-11-01"))
    .select("*", F.datediff(F.current_date(), "end_date").alias("days_since_end_date"))
)

attributed = (
    filtered
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.end_date") >= F.col("sub.start_date")) & (F.col("s.end_date") <= F.date_add(F.col("sub.end_date"), 3)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

with_windows = (
    attributed
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        "days_since_end_date",
        "subscription_id",
        "subscription_plan",
        F.last("monthly_fee", ignorenulls=True).over(w).alias("last_known_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w2).alias("last_known_monthly_fee_2"),
        F.percent_rank().over(w3).alias("monthly_fee_percent_rank"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

valid = (
    latest
    .filter(F.col("customer_id").isNotNull() & (~F.col("subscription_plan").eqNullSafe(F.lit("enterprise"))))
    .select(
        "*",
        F.add_months("start_date", 6).alias("start_date_plus_months"),
        F.round((F.col("last_known_monthly_fee") * 0.85), 2).alias("last_known_monthly_fee_scaled"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        "subscription_id",
        "subscription_plan",
        "last_known_monthly_fee",
        "last_known_monthly_fee_2",
        "monthly_fee_percent_rank",
        "dedup_rank",
        "start_date_plus_months",
        "last_known_monthly_fee_scaled",
        F.ntile(3).over(w5).alias("days_since_end_date_bucket"),
        F.min("subscription_id").over(w6).alias("status_min_subscription_id"),
        F.avg("monthly_fee_percent_rank").over(w7).alias("monthly_fee_percent_rank_avg_6"),
        F.round((F.col("last_known_monthly_fee") / F.sum("last_known_monthly_fee").over(w6)), 4).alias("last_known_monthly_fee_share"),
        F.ntile(3).over(w8).alias("last_known_monthly_fee_2_bucket"),
    )
)

result = (
    enriched
    .withColumn("quarter_start", F.date_trunc("QUARTER", "start_date_plus_months"))
    .withColumn("week_start", F.date_trunc("WEEK", "start_date_plus_months"))
    .groupingSets(
        [["quarter_start", "week_start", "subscription_id"], ["quarter_start", "week_start"]],
        "quarter_start",
        "week_start",
        "subscription_id",
    )
    .agg(
        F.avg(F.when(F.col("subscription_plan") == "enterprise", F.col("last_known_monthly_fee_share"))).alias("enterprise_avg_last_known_monthly_fee_share"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_rows"),
        F.count(F.when(F.col("subscription_plan") == "premium", 1)).alias("premium_count"),
        F.count_if(F.col("plan") == "standard").alias("standard_events"),
        F.round(F.sum("last_known_monthly_fee_scaled") / F.nullif(F.count(F.when(F.col("subscription_plan") == "standard", 1)), F.lit(0)), 2).alias("last_known_monthly_fee_scaled_per_standard"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
)
