from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("running_count_monthly_fee").desc(), "id")
w3 = Window.partitionBy("plan").orderBy(F.col("running_count_monthly_fee").desc(), "id")
w4 = Window.partitionBy("status").orderBy(F.col("monthly_fee").asc_nulls_last(), "id")
w5 = Window.partitionBy("status").orderBy(F.col("monthly_fee_minus_running_count_monthly_fee").desc_nulls_first())

scored = (
    df_subscriptions
    .filter(~F.col("status").isin(["ACTIVE", "PAUSED"]))
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        F.count("monthly_fee").over(w_grp).alias("running_count_monthly_fee"),
        F.min("monthly_fee").over(w_grp).alias("running_min_monthly_fee"),
        F.avg("monthly_fee").over(w_grp).alias("running_avg_monthly_fee"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "monthly_fee", "status", "running_count_monthly_fee")
)

base = (
    ranked
    .filter(F.col("status") != "CANCELLED")
    .withColumn("monthly_fee_times_running_count_monthly_fee", (F.col("monthly_fee") * F.col("running_count_monthly_fee")))
    .withColumn("monthly_fee_minus_running_count_monthly_fee", (F.col("monthly_fee") - F.col("running_count_monthly_fee")))
)

latest_subscriptions = (
    base
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "status",
        "running_count_monthly_fee",
        "monthly_fee_times_running_count_monthly_fee",
        "monthly_fee_minus_running_count_monthly_fee",
    )
)

latest_subscriptions_2 = (
    latest_subscriptions
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

daily = (
    latest_subscriptions_2
    .cube(
        F.when(F.col("running_count_monthly_fee") >= 14, "HIGH").otherwise("LOW").alias("running_count_monthly_fee_tier"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee_minus_running_count_monthly_fee")).otherwise(0)).alias("paused_monthly_fee_minus_running_count_monthly_fee"),
        F.avg("running_count_monthly_fee").alias("avg_running_count_monthly_fee"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
        F.percentile_approx("monthly_fee_minus_running_count_monthly_fee", 0.9).alias("monthly_fee_minus_running_count_monthly_fee_p90"),
        F.percentile_approx("monthly_fee_times_running_count_monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_times_running_count_monthly_fee_quartiles"),
        F.avg("monthly_fee_times_running_count_monthly_fee").alias("avg_monthly_fee_times_running_count_monthly_fee"),
    )
)

result = (
    daily
    .select(
        "running_count_monthly_fee_tier",
        "paused_monthly_fee_minus_running_count_monthly_fee",
        "avg_running_count_monthly_fee",
        "cancelled_events",
        "monthly_fee_minus_running_count_monthly_fee_p90",
        "monthly_fee_times_running_count_monthly_fee_quartiles",
        "avg_monthly_fee_times_running_count_monthly_fee",
    )
    .orderBy(F.col("monthly_fee_minus_running_count_monthly_fee_p90").desc_nulls_last())
)
