from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active", F.year("signup_date").alias("signup_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_consumer"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .filter(F.col("avg_credit_limit") >= 14400)
    .drop("avg_credit_limit")
    .orderBy(F.col("pct_consumer").desc(), "is_active", "signup_year")
    .limit(20)
)
