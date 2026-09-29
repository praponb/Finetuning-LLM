from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.job_title").isNull())
    .select(
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
)
