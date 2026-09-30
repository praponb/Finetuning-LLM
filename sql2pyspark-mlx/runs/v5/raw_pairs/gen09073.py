from pyspark.sql import functions as F

base = (
    df_customers
    .filter(~F.col("is_active"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.concat_ws("/", "country", "city").alias("country_city_key"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("age") >= 66, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
    )
    .agg(
        F.count_if(F.col("segment") == "Consumer").alias("consumer_events"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.percentile_approx("age", 0.9).alias("age_p90"),
    )
)
