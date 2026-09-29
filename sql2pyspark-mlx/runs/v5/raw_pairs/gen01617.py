from pyspark.sql import functions as F

avg_salary = df_employees.filter(F.col("dept_id").isin([7, 23, 35, 38])).agg(F.avg("salary")).first()[0]

result = (
    df_employees
    .filter(F.col("salary") >= avg_salary)
    .select("id", "salary", "is_remote")
    .orderBy(F.col("salary").desc())
)
