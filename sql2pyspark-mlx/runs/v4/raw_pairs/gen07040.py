from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["SG", "JP"]))
    .select("id", "name", "signup_date", "email", F.ceil("age").alias("age_whole"))
)
