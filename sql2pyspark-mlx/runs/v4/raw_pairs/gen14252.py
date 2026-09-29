from pyspark.sql import functions as F

cleaned = (
    df_subscriptions
    .filter(F.col("start_date") >= F.lit("2023-06-16").cast("date"))
    .select(
        "*",
        F.date_format("end_date", "yyyy-MM").alias("end_date_label"),
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

result = (
    cleaned
    .withColumn("week_start", F.date_trunc("WEEK", "start_date"))
    .withColumn("days_start_date_to_end_date_tier", F.when(F.col("days_start_date_to_end_date") >= 83, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["week_start", "days_start_date_to_end_date_tier"], ["week_start"], []],
        "week_start",
        "days_start_date_to_end_date_tier",
    )
    .agg(
        F.sum(F.when(F.col("plan") == "standard", F.col("days_start_date_to_end_date"))).alias("standard_total_days_start_date_to_end_date"),
        F.round(F.sum("days_start_date_to_end_date") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("days_start_date_to_end_date_per_basic"),
        F.avg(F.when(F.col("plan") == "enterprise", F.col("days_start_date_to_end_date"))).alias("enterprise_avg_days_start_date_to_end_date"),
    )
    .filter(F.col("enterprise_avg_days_start_date_to_end_date") >= 0)
    .orderBy("enterprise_avg_days_start_date_to_end_date")
)
