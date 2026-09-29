from pyspark.sql import functions as F

avg_salary = df_employees.filter(F.col("dept_id") == 15).agg(F.avg("salary")).first()[0]

result = (
    df_employees
    .filter(F.col("salary") > avg_salary)
    .select("id", "salary", "manager_id")
    .orderBy(F.col("salary").desc())
)
