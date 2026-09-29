from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Tokyo")
    .select("email", F.concat_ws(",", "country", "email").alias("country_email_key"))
    .orderBy("country_email_key")
)
