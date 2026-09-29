from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Bangkok")
    .groupBy(
        F.when(F.col("credit_limit") >= 17900, "high").when(F.col("credit_limit") >= 8000, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
    .filter(F.col("stddev_credit_limit") > 46)
    .select("credit_limit_bucket", "stddev_credit_limit", "min_credit_limit")
)
