from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 12800)
    .groupBy(
        F.when(F.col("age") >= 68, "high").when(F.col("age") >= 34, "mid").otherwise("low").alias("age_bucket"),
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
        "segment",
    )
    .agg(F.countDistinct("credit_limit").alias("unique_citys"))
    .filter(F.col("unique_citys") > 2)
    .orderBy(F.col("signup_month").desc(), "age_bucket", "segment")
    .limit(10)
)
