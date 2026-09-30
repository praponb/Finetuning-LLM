from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date_plus_44d", "id")
w_recent = Window.orderBy("end_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("monthly_fee", "id")
w4 = Window.orderBy("running_sum_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("last_known_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("monthly_fee", "id").rowsBetween(-6, -1)

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        F.date_add("end_date", 44).alias("end_date_plus_44d"),
        F.last_day("end_date").alias("end_date_month_end"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "end_date", "monthly_fee", "end_date_month_end")
)

enriched = (
    latest
    .select(
        "id",
        "monthly_fee",
        F.avg("monthly_fee").over(w_recent).alias("running_avg_monthly_fee"),
        F.sum("monthly_fee").over(w_recent).alias("running_sum_monthly_fee"),
        F.max("monthly_fee").over(w_recent).alias("running_max_monthly_fee"),
    )
)

cleaned = (
    enriched
    .filter(F.col("monthly_fee").isNotNull())
    .select(
        "id",
        "monthly_fee",
        "running_sum_monthly_fee",
        "running_max_monthly_fee",
        F.greatest("running_avg_monthly_fee", "running_max_monthly_fee").alias("greatest_running_avg_monthly_fee_running_max_monthly_fee"),
    )
)

enriched_subscriptions = (
    cleaned
    .select(
        "id",
        "monthly_fee",
        "greatest_running_avg_monthly_fee_running_max_monthly_fee",
        F.lead("monthly_fee").over(w3).alias("next_monthly_fee"),
        F.lag("monthly_fee").over(w3).alias("prev_monthly_fee"),
        F.lead("running_max_monthly_fee").over(w3).alias("next_running_max_monthly_fee"),
        F.row_number().over(w3).alias("event_seq"),
        F.last("monthly_fee", ignorenulls=True).over(w4).alias("last_known_monthly_fee"),
    )
)

metrics = (
    enriched_subscriptions
    .filter(F.col("prev_monthly_fee") > 130)
    .select(
        "id",
        "next_monthly_fee",
        "next_running_max_monthly_fee",
        F.sum("event_seq").over(w5).alias("running_event_seq"),
        F.avg("monthly_fee").over(w6).alias("monthly_fee_avg_6_prior"),
    )
)

cleaned_subscriptions = (
    metrics
    .filter(F.col("next_running_max_monthly_fee").isNotNull())
    .select(
        "id",
        "next_monthly_fee",
        "next_running_max_monthly_fee",
        "running_event_seq",
        "monthly_fee_avg_6_prior",
        F.round((F.col("monthly_fee_avg_6_prior") * 0.85), 2).alias("monthly_fee_avg_6_prior_scaled"),
    )
)

result = (
    cleaned_subscriptions
    .select(
        "id",
        "next_monthly_fee",
        "next_running_max_monthly_fee",
        "running_event_seq",
        "monthly_fee_avg_6_prior",
    )
    .orderBy(F.col("next_monthly_fee").desc_nulls_last())
)
