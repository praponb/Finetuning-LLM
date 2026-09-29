from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("segment").isNotNull() & F.col("is_active"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.coalesce("email", F.lit("Unknown")).alias("email_filled"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("YEAR", "signup_date").alias("year_start"))
    .agg(
        F.count_if(F.col("country") == "UK").alias("uk_events"),
        F.bit_or("age").alias("age_bit_or"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.sum(F.when(F.col("country") == "SG", F.col("age")).otherwise(0)).alias("sg_age"),
    )
    .orderBy("year_start")
)
