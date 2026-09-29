from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") <= 43) & (F.col("age") != 54))
    .select("id", "credit_limit", F.trim("email").alias("clean_email"))
)
