from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.dept_id") == 29)
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
    .orderBy(F.col("id").desc())
)
