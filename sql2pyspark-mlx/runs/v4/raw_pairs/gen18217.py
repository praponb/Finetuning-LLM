from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.email").alias("employee_email"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.level").alias("manager_level"),
        F.col("m.job_title").alias("manager_job_title"),
    )
    .orderBy("manager_dept_id")
)
