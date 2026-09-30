from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.max("age").alias("max_age"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
    .orderBy("max_age", F.col("stddev_credit_limit").desc(), "signup_month")
    .limit(10)
)
