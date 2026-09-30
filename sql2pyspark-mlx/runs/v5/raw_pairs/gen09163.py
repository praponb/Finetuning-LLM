from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy("monthly_fee_rounded_int", "id")
w_ordered = Window.partitionBy("status").orderBy(F.col("days_since_start_date").desc(), "id")
w3 = Window.partitionBy("plan", "customer_id").orderBy("end_date", "id").rowsBetween(-3, -1)
w_ordered_2 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status")
w6 = Window.partitionBy("customer_id").orderBy(F.col("days_since_start_date_p95").desc())

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-05-28").cast("date"))
    .select("*", F.concat_ws("-", "status", "plan").alias("status_plan_key"))
)

base = (
    cleaned
    .filter((F.col("end_date") >= F.lit("2023-04-11").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "status_plan_key",
        F.round("monthly_fee", 0).alias("monthly_fee_rounded"),
        F.date_format("start_date", "yyyy-MM-dd").alias("start_date_label"),
    )
)

valid = (
    base
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
        F.col("monthly_fee_rounded").cast("int").alias("monthly_fee_rounded_int"),
    )
)

scored = (
    valid
    .filter(F.col("days_since_start_date") == 67)
    .withColumn("prev_monthly_fee_rounded_int", F.lag("monthly_fee_rounded_int", 1).over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("days_since_start_date_avg_3_prior", F.avg("days_since_start_date").over(w3))
    .withColumn("first_known_monthly_fee_rounded_int", F.first("monthly_fee_rounded_int", ignorenulls=True).over(w_ordered_2))
    .withColumn("status_avg_monthly_fee_rounded_int", F.avg("monthly_fee_rounded_int").over(w5))
)

result = (
    scored
    .groupBy(
        "customer_id",
        F.when(F.col("days_since_start_date_avg_3_prior") >= 72, "PREMIUM").otherwise("STANDARD").alias("days_since_start_date_avg_3_prior_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("days_since_start_date", 0.95).alias("days_since_start_date_p95"),
    )
    .withColumn("days_since_start_date_p95_percent_rank", F.percent_rank().over(w6))
    .orderBy("customer_id")
)
