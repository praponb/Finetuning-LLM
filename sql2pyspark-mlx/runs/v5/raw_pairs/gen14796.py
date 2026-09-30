from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("end_date_year_filled")

filtered = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2023-09-17").cast("date"))
    .select("*", F.year("end_date").alias("end_date_year"))
)

valid = (
    filtered
    .filter(F.col("end_date_year").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        "end_date_year",
        F.coalesce("end_date_year", F.lit(0)).alias("end_date_year_filled"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date_year",
        F.sum("end_date_year").over(w).alias("cumulative_end_date_year"),
        F.percent_rank().over(w2).alias("end_date_year_filled_percent_rank"),
    )
)

with_window = (
    enriched
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.start_date") >= F.col("sub.start_date")) & (F.col("a.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee"),
    )
)

base = (
    with_window
    .filter(F.col("subscription_plan") == "basic")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date_year",
        "cumulative_end_date_year",
        "end_date_year_filled_percent_rank",
        "subscription_id",
        "subscription_plan",
        "monthly_fee",
        (F.col("subscription_id") / F.nullif("end_date_year", F.lit(0))).alias("subscription_id_per_end_date_year"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("monthly_fee") >= 244, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("subscription_plan") == "premium", 1)).alias("premium_count"),
        F.round(F.avg("cumulative_end_date_year"), 2).alias("avg_cumulative_end_date_year"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
    )
    .orderBy(F.col("enterprise_events").desc())
)
