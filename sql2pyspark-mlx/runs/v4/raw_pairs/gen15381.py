from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("city").isin(["London", "Bangkok", "Singapore"]) & F.col("email").like("%@yahoo.com"))
    .select("signup_date")
    .orderBy("signup_date")
)
