from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("greatest_monthly_fee_start_date_day").desc(), "id")
w2 = Window.orderBy("adjusted_monthly_fee", "id")
w3 = Window.orderBy("start_date_day", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_subscriptions
    .filter(F.month("end_date").isin([1, 7, 12]))
    .withColumn("days_start_date_to_end_date", F.datediff("end_date", "start_date"))
    .withColumn("start_date_day", F.dayofmonth("start_date"))
)

cleaned = (
    filtered
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "start_date_day",
        F.greatest("monthly_fee", "start_date_day").alias("greatest_monthly_fee_start_date_day"),
        (F.col("start_date_day") * F.col("monthly_fee")).alias("start_date_day_times_monthly_fee"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

cleaned_subscriptions = (
    ranked
    .filter(F.col("start_date") > F.lit("2023-09-26").cast("date"))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "start_date_day",
        "greatest_monthly_fee_start_date_day",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
    )
)

filtered_subscriptions = (
    cleaned_subscriptions
    .filter(F.col("greatest_monthly_fee_start_date_day").between(39, 43))
    .withColumn("plan_upper", F.upper("plan"))
    .withColumn("monthly_fee_whole_times_monthly_fee", (F.col("monthly_fee_whole") * F.col("monthly_fee")))
)

cleaned_subscriptions_2 = (
    filtered_subscriptions
    .filter(F.col("start_date") > F.lit("2023-08-31").cast("date"))
    .select(
        "id",
        "start_date",
        "end_date",
        "start_date_day",
        "monthly_fee_whole",
        "plan_upper",
        F.round((F.col("monthly_fee") * 1.07), 2).alias("adjusted_monthly_fee"),
    )
)

enriched = (
    cleaned_subscriptions_2
    .select(
        "id",
        "start_date",
        "end_date",
        "start_date_day",
        "monthly_fee_whole",
        "adjusted_monthly_fee",
        F.ntile(5).over(w2).alias("adjusted_monthly_fee_bucket"),
        F.avg("monthly_fee_whole").over(w3).alias("monthly_fee_whole_avg_6"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("monthly_fee_whole_avg_6") >= 87, "HIGH").otherwise("LOW").alias("monthly_fee_whole_avg_6_tier"),
    )
    .agg(
        F.round(F.avg("adjusted_monthly_fee_bucket"), 2).alias("avg_adjusted_monthly_fee_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("adjusted_monthly_fee", [0.25, 0.5, 0.75]).alias("adjusted_monthly_fee_quartiles"),
        F.percentile_approx("start_date_day", [0.25, 0.5, 0.75]).alias("start_date_day_quartiles"),
        F.round(F.avg("adjusted_monthly_fee"), 2).alias("avg_adjusted_monthly_fee"),
    )
)
