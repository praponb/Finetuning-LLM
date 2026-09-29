from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("element"))
)

base = (
    lines
    .filter((F.col("effective_from") > F.lit("2024-06-06").cast("date")) & (F.col("effective_from") >= "2023-04-01"))
    .select("*", F.upper("plan").alias("plan_upper"))
)

filtered = (
    base
    .filter(F.col("effective_from") >= F.lit("2024-04-21").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "plan_upper",
        F.date_sub("effective_from", 66).alias("effective_from_minus_66d"),
    )
)

cleaned = (
    filtered
    .filter(F.col("effective_from") >= F.lit("2024-05-30").cast("date"))
    .withColumn("days_since_effective_from_minus_66d", F.datediff(F.current_date(), "effective_from_minus_66d"))
)

valid = (
    cleaned
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-05-19"))) & (F.col("days_since_effective_from_minus_66d") != 31))
    .select(
        "id",
        "plan",
        "effective_from",
        "effective_from_minus_66d",
        F.dayofweek("effective_from").alias("effective_from_dayofweek"),
    )
)

result = (
    valid
    .groupBy("plan")
    .agg(
        F.percentile_approx("effective_from_dayofweek", 0.5).alias("effective_from_dayofweek_p50"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.percentile_approx("effective_from_dayofweek", 0.9).alias("effective_from_dayofweek_p90"),
    )
)
