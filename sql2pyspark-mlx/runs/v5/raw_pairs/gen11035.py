from pyspark.sql import functions as F

max_salary = df_employees.filter(F.col("salary") >= 130000).agg(F.max("salary")).first()[0]

result = (
    df_employees
    .filter(F.col("salary") > max_salary)
    .select("id", "salary", "job_title")
    .orderBy("job_title")
)
