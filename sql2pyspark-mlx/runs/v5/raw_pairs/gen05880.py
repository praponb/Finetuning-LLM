from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") >= "2025-03-01") & (F.col("salary") > 129000) & (F.col("manager_id") == 21))
    .select("id", "email")
    .orderBy("email")
)
