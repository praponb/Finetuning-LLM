from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.length("email") >= 12) & ~F.col("is_active"))
    .groupBy(
        F.when(F.col("credit_limit") >= 16100, "high").when(F.col("credit_limit") >= 7300, "mid").otherwise("low").alias("credit_limit_bucket"),
        "country",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_consumer"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
    .filter(F.col("pct_consumer") >= 67)
    .orderBy("country", F.col("pct_consumer").desc(), "credit_limit_bucket")
    .limit(10)
)
