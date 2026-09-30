from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") >= "2023-09-30")
    .select("id", "job_title", "level", "email")
    .orderBy(F.col("email").desc(), "id")
    .limit(10)
)
