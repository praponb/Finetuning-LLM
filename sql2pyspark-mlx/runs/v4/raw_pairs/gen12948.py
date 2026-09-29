from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan", "status").orderBy(F.col("start_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w_recent = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")

valid = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2024-03-25").cast("date")) & F.col("customer_id").isNotNull() & (F.col("status") == "CANCELLED"))
    .select(
        "*",
        F.last_day("end_date").alias("end_date_month_end"),
        F.date_trunc("YEAR", "start_date").alias("start_date_period"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.sum("monthly_fee").over(w_running).alias("monthly_fee_sum_7d"),
        F.ntile(3).over(w2).alias("monthly_fee_bucket"),
        F.sum("monthly_fee").over(w_recent).alias("running_monthly_fee"),
    )
)

base = (
    metrics
    .filter((F.col("end_date") >= F.to_date(F.lit("2024-04-17"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "end_date_month_end",
        "monthly_fee_bucket",
        F.when(F.col("plan").isin(["enterprise", "standard"]), "Core").otherwise("Other").alias("plan_group"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .withColumn("year_start", F.date_trunc("YEAR", "end_date_month_end"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "end_date_month_end"))
    .rollup("year_start", "quarter_start")
    .agg(
        F.avg(F.when(F.col("status") == "PAUSED", F.col("monthly_fee"))).alias("paused_avg_monthly_fee"),
        F.percentile_approx("monthly_fee_bucket", 0.9).alias("monthly_fee_bucket_p90"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("monthly_fee_bucket_p90").desc())
)
