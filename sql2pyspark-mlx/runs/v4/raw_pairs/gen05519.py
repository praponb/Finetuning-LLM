from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@startup.io") | F.col("signup_date").between("2023-04-01", "2024-03-01"))
    .select("id", "signup_date", "city")
    .orderBy("id")
    .limit(10)
)
