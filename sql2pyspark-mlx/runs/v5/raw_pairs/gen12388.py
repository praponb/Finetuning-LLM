from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_bucket").asc_nulls_last())

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2023-11-07").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.dayofweek("start_date").alias("start_date_dayofweek"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.ntile(10).over(w).alias("monthly_fee_bucket"),
        F.count("start_date_dayofweek").over(w2).alias("plan_count_start_date_dayofweek"),
        F.sum("monthly_fee").over(w3).alias("running_monthly_fee"),
        F.stddev_samp("monthly_fee").over(w4).alias("status_stddev_samp_monthly_fee"),
        F.avg("monthly_fee").over(w5).alias("monthly_fee_avg_2_prior"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.rank().over(w6))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    ranked
    .filter(F.col("end_date") >= F.lit("2023-06-20").cast("date"))
    .withColumn("status_group", F.when(F.col("status").isin(["PAUSED", "EXPIRED"]), "Tier 1").otherwise("Other"))
    .withColumn("running_monthly_fee_plus_plan_count_start_date_dayofweek", (F.col("running_monthly_fee") + F.col("plan_count_start_date_dayofweek")))
)

result = (
    valid
    .groupBy(F.date_trunc("YEAR", "end_date").alias("year_start"))
    .agg(
        F.count_if(F.col("status") == "ACTIVE").alias("active_events"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.round(F.sum("running_monthly_fee") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("running_monthly_fee_per_enterprise"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
        F.round(F.avg("plan_count_start_date_dayofweek"), 2).alias("avg_plan_count_start_date_dayofweek"),
    )
)
