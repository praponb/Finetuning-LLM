from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("email").like("%@company.co"))

agg = (
    filtered_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(F.max("signup_date").alias("latest_signup_date"))
)

result = agg.select("signup_month", "latest_signup_date")
