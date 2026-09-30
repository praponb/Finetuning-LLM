from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") <= "2023-08-01") & (F.col("country") != "US") & (F.year("signup_date") >= 2025))
    .withColumn("email_upper", F.upper("email"))
    .withColumn("email_title", F.initcap(F.trim("email")))
    .orderBy(F.col("name").desc())
)
