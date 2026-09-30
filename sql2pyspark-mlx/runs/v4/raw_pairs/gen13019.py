from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
    )
    .filter(F.col("pct_singapore") > 52)
    .select("signup_month", "pct_singapore")
    .orderBy(F.col("signup_month").desc(), F.col("pct_singapore").desc())
    .limit(10)
)
