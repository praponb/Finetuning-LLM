from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.job_title") == "Intern")
    .select(
        F.col("e.id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
)
