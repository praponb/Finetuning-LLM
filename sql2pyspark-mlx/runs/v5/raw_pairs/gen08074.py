from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") < 55) & F.col("signup_date").between("2024-10-30", "2025-06-12") & (F.col("signup_date") >= "2024-04-01"))
    .withColumn("clean_email", F.trim("email"))
    .withColumn("name_city", F.concat("name", F.lit(" - "), "city"))
    .orderBy("id", "country")
    .limit(20)
)
