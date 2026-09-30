from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") <= 12800) & F.col("email").isNull())
    .groupBy(F.year("signup_date").alias("signup_year"), "country")
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
    .orderBy("min_credit_limit", "signup_year", "country")
    .limit(5)
)
