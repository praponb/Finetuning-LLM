from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment").orderBy(F.col("age").asc_nulls_last())
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("city").orderBy("running_credit_limit", "id")

cleaned = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "*",
        F.lower("segment").alias("segment_lower"),
        F.length("country").alias("country_length"),
    )
)

filtered = (
    cleaned
    .filter((F.col("signup_date") > F.lit("2023-07-16").cast("date")) & F.col("city").isNotNull())
    .select(
        "*",
        F.trim("city").alias("clean_city"),
        F.date_format("signup_date", "yyyy-MM-dd").alias("signup_date_label"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "email",
        "city",
        "credit_limit",
        "country_length",
        "clean_city",
        F.sum("credit_limit").over(w).alias("running_credit_limit"),
        F.rank().over(w2).alias("age_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "city",
        "country_length",
        "clean_city",
        "age_rank",
        F.sum("running_credit_limit").over(w3).alias("city_sum_running_credit_limit"),
        F.ntile(4).over(w4).alias("running_credit_limit_bucket"),
    )
)

result = (
    metrics
    .groupBy("city")
    .agg(
        F.percentile_approx("country_length", [0.25, 0.5, 0.75]).alias("country_length_quartiles"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("city_sum_running_credit_limit"))).alias("tokyo_total_city_sum_running_credit_limit"),
        F.percentile_approx("running_credit_limit_bucket", 0.5).alias("running_credit_limit_bucket_p50"),
        F.percentile_approx("running_credit_limit_bucket", 0.9).alias("running_credit_limit_bucket_p90"),
        F.percentile_approx("age_rank", 0.95).alias("age_rank_p95"),
    )
    .filter(F.col("running_credit_limit_bucket_p90") > 0)
)
