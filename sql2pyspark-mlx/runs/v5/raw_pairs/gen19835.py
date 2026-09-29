from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("name") < 13)
    .groupBy(
        F.when(F.col("credit_limit") >= 16600, "high").when(F.col("credit_limit") >= 3500, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
    )
    .orderBy("credit_limit_bucket")
)
