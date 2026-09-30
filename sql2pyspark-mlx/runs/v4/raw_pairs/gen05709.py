from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.length("name") < 6) & F.col("name").isNull() & (F.year("signup_date") >= 2025))
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"), "segment")
    .agg(
        F.min("age").alias("min_age"),
        F.max("signup_date").alias("last_signup_date"),
        F.max("signup_date").alias("latest_signup_date"),
    )
    .filter(F.col("min_age") < 67)
    .orderBy("latest_signup_date", "signup_month", "segment")
    .limit(1)
)
