from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") < 7200)
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
        F.round(((100 * F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_consumer"),
        F.sum("age").alias("total_age"),
    )
    .orderBy(F.col("credit_limit_range").desc())
)
