from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") != 48)
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .pivot("city", ["Singapore", "Tokyo", "Bangkok"])
    .agg(F.count("credit_limit"))
    .orderBy("signup_month")
)
