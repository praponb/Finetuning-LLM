from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(31, 59))
    .select(F.lit("customer").alias("source"), "signup_date")
    .union(df_employees.select(F.lit("employee").alias("source"), "hire_date"))
    .distinct()
    .orderBy("signup_date")
)
