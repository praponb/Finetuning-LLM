from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-10-08").cast("date"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.initcap(F.trim("country")).alias("country_title"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("age") >= 58, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
    )
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.stddev_samp("age").alias("age_stddev"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
    )
)
