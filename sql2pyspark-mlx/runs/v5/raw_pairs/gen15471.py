from pyspark.sql import functions as F

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-04-08").cast("date")) & F.col("segment").isNotNull())
    .select(
        "*",
        F.coalesce("segment", F.lit("Unknown")).alias("segment_filled"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

result = (
    valid
    .groupBy(F.date_trunc("YEAR", "signup_date").alias("year_start"))
    .agg(
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
    .filter(F.col("num_segments") < 1000)
    .orderBy(F.col("credit_limit_stddev").desc_nulls_last())
)
