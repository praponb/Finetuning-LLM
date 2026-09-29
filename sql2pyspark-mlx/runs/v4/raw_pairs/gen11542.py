from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter((F.col("segment") != "Consumer") & (F.year("signup_date") >= 2023))
)

signup_year_summary = (
    recent_customers
    .groupBy(F.year("signup_date").alias("signup_year"), "country")
    .agg(F.sum("age").alias("total_age"))
)

result = (
    signup_year_summary
    .orderBy(F.col("total_age").desc(), "signup_year", "country")
    .limit(5)
)
