from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.month("signup_date").isin([3, 4, 12]) & (F.col("signup_date") >= "2024-06-17"))
)

summary = (
    base
    .groupBy("is_active")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)

result = (
    summary
    .filter(F.col("pct_london") >= 63)
    .orderBy(F.col("pct_london").desc())
)
