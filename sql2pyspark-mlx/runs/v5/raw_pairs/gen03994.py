from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(14600, 15900) & F.col("age").between(45, 57) & F.col("country").isin(["SG", "UK", "TH"]))
    .groupBy(
        F.when(F.col("credit_limit") >= 16700, "high").when(F.col("credit_limit") >= 7300, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
        F.sum("age").alias("total_age"),
    )
    .orderBy("stddev_credit_limit")
)
