from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([1, 2, 3]) & F.col("age").between(46, 60) & F.col("age").between(40, 51))
    .groupBy(
        F.date_format("signup_date", "yyyy-MM").alias("signup_month"),
        F.when(F.col("age") >= 57, "high").when(F.col("age") >= 33, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.sum("age").alias("total_age"))
    .filter(F.col("total_age") < 180)
    .orderBy("age_bucket", "signup_month")
    .limit(10)
)
