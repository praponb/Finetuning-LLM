from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 13400, "high").when(F.col("credit_limit") >= 4600, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.sum("credit_limit").alias("total_credit_limit"))
    .orderBy("credit_limit_bucket")
)
