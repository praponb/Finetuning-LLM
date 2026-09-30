from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") > "2025-08-01")
    .withColumn("clean_email", F.trim("email"))
    .orderBy("clean_email", "id")
    .limit(10)
)
