from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") < "2023-07-01")
    .withColumn("greatest_age_credit_limit", F.greatest("age", "credit_limit"))
    .withColumn("country_title", F.initcap(F.trim("country")))
)
