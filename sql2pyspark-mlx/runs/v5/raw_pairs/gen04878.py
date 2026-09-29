from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.is_remote"))
    .select(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.email").alias("manager_email"),
    )
    .orderBy("employee_salary", F.col("employee_job_title").desc())
)
