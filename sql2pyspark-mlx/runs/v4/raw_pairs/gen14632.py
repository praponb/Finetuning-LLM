from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") <= 39)
    .select("signup_date")
    .unionByName(df_employees.select(F.col("hire_date").alias("signup_date")))
    .distinct()
    .orderBy(F.col("signup_date").desc())
)
