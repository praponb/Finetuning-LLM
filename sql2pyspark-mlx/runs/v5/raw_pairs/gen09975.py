from pyspark.sql import functions as F

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-07-12"))) & F.col("city").isNotNull())
    .withColumn("age_adjusted", F.round((F.col("age") * 1.15), 2))
)

base = (
    cleaned
    .filter((F.col("signup_date") > F.lit("2023-06-05").cast("date")) & F.col("city").isNotNull())
    .withColumn("country_email", F.concat("country", F.lit(" - "), "email"))
    .withColumn("name_slug", F.regexp_replace("name", " ", "_"))
)

filtered = (
    base
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-05-18"))) & F.col("city").isNotNull())
    .select(
        "*",
        F.concat("segment", F.lit("/"), "name").alias("segment_name"),
        F.weekofyear("signup_date").alias("signup_date_weekofyear"),
    )
)

kept_keys = (
    filtered
    .select("id", "segment")
    .subtract(
        df_customers
        .filter(F.col("email").isNotNull())
        .select("id", "segment")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") >= F.col("s.signup_date")), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "name",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "age_adjusted",
        "country_email",
    )
)
