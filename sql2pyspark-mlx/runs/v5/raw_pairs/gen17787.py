from pyspark.sql import functions as F

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-06-19").cast("date")) & F.col("country").isin(["DE", "TH", "JP"]))
    .select(
        "id",
        "name",
        "signup_date",
        "is_active",
        "credit_limit",
        F.length("email").alias("email_length"),
        F.lower("segment").alias("segment_lower"),
    )
)

result = (
    valid
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 13200, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "signup_date"))
    .rollup("credit_limit_tier", "quarter_start")
    .agg(
        F.sum("email_length").alias("total_email_length"),
        F.percentile_approx("credit_limit", 0.5).alias("credit_limit_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("email_length", [0.25, 0.5, 0.75]).alias("email_length_quartiles"),
        F.grouping("credit_limit_tier").alias("grouping_level"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
