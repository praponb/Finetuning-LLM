from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.dept_id").isin([6, 20, 31]))
    .select(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
)
