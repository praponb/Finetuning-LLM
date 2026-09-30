from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") != 38) & (F.upper(F.trim("email")) == "EMMA@OUTLOOK.COM"))
    .select("signup_date")
    .orderBy(F.col("signup_date").desc())
)
