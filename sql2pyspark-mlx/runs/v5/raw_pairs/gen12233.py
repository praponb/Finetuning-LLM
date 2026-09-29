from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(40, 53) & ~F.col("name").like("%Grac%"))
    .groupBy("is_active")
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
        F.max("credit_limit").alias("max_credit_limit"),
    )
    .filter(F.col("max_credit_limit") < 6500)
    .orderBy(F.col("max_credit_limit").desc())
)
