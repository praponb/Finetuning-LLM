from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(38, 60) | (F.col("country") == "TH"))
    .select("name", "age", F.concat_ws("/", "city", "email").alias("city_email_key"))
)
