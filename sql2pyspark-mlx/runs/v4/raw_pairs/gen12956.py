from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "DE")
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .pivot("city", ["Singapore", "London", "New York", "Bangkok"])
    .agg(F.max("credit_limit"))
    .orderBy("signup_month")
)
