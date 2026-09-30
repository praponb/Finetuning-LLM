from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.salary") > 92000)
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
    .orderBy("employee_dept_id", "id")
    .limit(5)
)
