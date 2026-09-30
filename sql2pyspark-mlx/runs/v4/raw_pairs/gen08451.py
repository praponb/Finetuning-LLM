from pyspark.sql import functions as F

with_dims = (
    df_employees
    .alias("src")
    .join(df_employees.alias("b"), F.col("src.manager_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.salary").alias("employee_salary"))
)

result = (
    with_dims
    .select("id", "email", "manager_id", "hire_date", "is_remote", "level")
    .orderBy(F.col("manager_id").desc())
)
