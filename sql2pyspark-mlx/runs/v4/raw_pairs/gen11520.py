from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-10-16").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.coalesce("name", F.lit("N/A")).alias("name_filled"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

cleaned_customers = (
    cleaned
    .filter(F.col("age") <= 39)
    .select("*", F.length("segment").alias("segment_length"))
)

base = (
    cleaned_customers
    .filter(F.col("credit_limit").between(5100, 7900))
    .withColumn("segment_length_per_credit_limit", F.round((F.col("segment_length") / F.col("credit_limit")), 2))
)

valid = (
    base
    .filter(F.col("segment_length_per_credit_limit").isNotNull())
    .withColumn("age_plus_credit_limit", (F.col("age") + F.col("credit_limit")))
    .withColumn("segment_length_per_age", F.round((F.col("segment_length") / F.col("age")), 2))
)

result = (
    valid
    .select(
        "id",
        "segment",
        "segment_length_per_credit_limit",
        "age_plus_credit_limit",
        "credit_limit",
    )
    .unpivot(
        ["id", "segment"],
        ["segment_length_per_credit_limit", "age_plus_credit_limit", "credit_limit"],
        "metric_name",
        "val",
    )
)
