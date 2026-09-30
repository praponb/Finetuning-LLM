from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", F.month("signup_date").alias("signup_month"))
    .agg(
        F.min("age").alias("min_age"),
        F.round(((100 * F.sum(F.when(F.col("country") == "UK", 1).otherwise(0))) / F.count("*")), 1).alias("pct_uk"),
    )
    .orderBy(F.col("pct_uk").desc(), "segment", "signup_month")
    .limit(1)
)
