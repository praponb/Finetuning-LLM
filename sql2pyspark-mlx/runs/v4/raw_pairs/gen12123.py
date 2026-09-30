from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("element"))
)

cleaned = (
    lines
    .filter(F.col("element").isNull())
    .select(
        "*",
        F.datediff(F.current_date(), "effective_from").alias("days_since_effective_from"),
    )
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("effective_from") > F.lit("2024-06-10").cast("date"))
    .select(
        "*",
        F.least("days_since_effective_from", "element").alias("least_days_since_effective_from_element"),
        F.floor("days_since_effective_from").alias("days_since_effective_from_whole"),
    )
)

grouped = (
    cleaned_price_tiers
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.round(F.avg("days_since_effective_from_whole"), 2).alias("avg_days_since_effective_from_whole"),
        F.percentile_approx("days_since_effective_from_whole", 0.95).alias("days_since_effective_from_whole_p95"),
        F.round(F.avg("least_days_since_effective_from_element"), 2).alias("avg_least_days_since_effective_from_element"),
        F.percentile_approx("days_since_effective_from", 0.9).alias("days_since_effective_from_p90"),
        F.count("*").alias("row_count"),
    )
)

result = (
    grouped
    .select(
        "quarter_start",
        "days_since_effective_from_whole_p95",
        "days_since_effective_from_p90",
    )
)
