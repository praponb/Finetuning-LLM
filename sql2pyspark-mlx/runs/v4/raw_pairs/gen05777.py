from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.salary").alias("employee_salary"),
        F.col("e.level").alias("employee_level"),
        F.col("m.email").alias("manager_email"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
    .orderBy("manager_job_title")
)
