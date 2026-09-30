from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.manager_id").isNotNull() & F.col("m.dept_id").isin([15, 25, 29]))
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.salary").alias("manager_salary"),
    )
    .orderBy("employee_full_name")
)
