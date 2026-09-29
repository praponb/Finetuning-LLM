from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-11-13").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "*",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

result = (
    base
    .groupBy(
        F.date_trunc("WEEK", "shipped_date").alias("week_start"),
        F.date_trunc("MONTH", "shipped_date_month_end").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.max(F.when(F.col("carrier") == "Kerry", F.col("cost"))).alias("kerry_max_cost"),
        F.count_if(F.col("status") == "DELIVERED").alias("delivered_events"),
        F.count_if(F.col("status") == "LOST").alias("lost_events"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost_2"),
    )
    .filter(F.col("delivered_events") > 1)
    .orderBy(F.col("cost_p50").asc_nulls_last())
)
