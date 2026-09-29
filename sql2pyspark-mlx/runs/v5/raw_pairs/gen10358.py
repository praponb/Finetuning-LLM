from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.length(F.col("m.email")) < 5)
    .select(
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.email").alias("manager_email"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.full_name").alias("manager_full_name"),
    )
    .orderBy(F.col("manager_salary").desc())
)
