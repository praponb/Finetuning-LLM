from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower("name").like("%lic%") & F.col("email").isNotNull())
    .select("name", F.last_day("signup_date").alias("signup_date_month_end"))
    .orderBy("signup_date_month_end", "name")
)
