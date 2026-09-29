from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("country")
    .subtract(
        df_customers
        .filter(F.col("credit_limit") < 11300)
        .select("country")
    )
)

verified = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("age") >= 57, "HIGH").otherwise("LOW").alias("age_tier"),
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
    )
    .agg(
        F.avg(F.when(F.col("city") == "Singapore", F.col("credit_limit"))).alias("singapore_avg_credit_limit"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.bit_or("age").alias("age_bit_or"),
    )
)
