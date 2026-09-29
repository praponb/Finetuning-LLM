from pyspark.sql import functions as F

min_signup_date = df_customers.agg(F.min("signup_date")).first()[0]

result = (
    df_customers
    .filter(F.col("signup_date") <= min_signup_date)
    .select("id", "signup_date")
)
