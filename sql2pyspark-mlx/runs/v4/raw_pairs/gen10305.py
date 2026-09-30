from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-6, -1)

metrics = (
    df_subscriptions
    .select(
        "*",
        F.stddev_samp("monthly_fee").over(w).alias("plan_stddev_samp_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee_2"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_6_prior"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.name"))
)

valid = (
    enriched
    .filter(F.col("end_date") > F.lit("2023-10-13").cast("date"))
    .select(
        "*",
        F.round((F.col("running_monthly_fee") * 1.07), 2).alias("adjusted_running_monthly_fee"),
    )
)

cleaned = (
    valid
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-09-14"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "start_date",
        "end_date",
        "plan_stddev_samp_monthly_fee",
        "running_monthly_fee",
        "running_monthly_fee_2",
        "monthly_fee_avg_6_prior",
        "adjusted_running_monthly_fee",
        "days_end_date_to_start_date",
    )
    .orderBy(F.col("start_date").desc(), F.col("id").desc())
    .limit(20)
)
