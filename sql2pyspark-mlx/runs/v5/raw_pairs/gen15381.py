from pyspark.sql import functions as F

min_salary = df_employees.agg(F.min("salary")).first()[0]

result = (
    df_employees
    .filter(F.col("salary") <= min_salary)
    .select("id", "salary", "dept_id", "full_name")
    .orderBy("full_name", F.col("salary").desc())
)
