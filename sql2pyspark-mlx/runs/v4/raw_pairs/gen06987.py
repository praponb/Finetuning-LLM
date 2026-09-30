from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 72, "high").when(F.col("age") >= 35, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
    .filter(F.col("avg_credit_limit") > 67)
    .select("age_bucket", "avg_credit_limit")
)
