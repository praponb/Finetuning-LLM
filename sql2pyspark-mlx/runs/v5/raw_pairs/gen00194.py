from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.id"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("mgr.level").alias("manager_level"),
    )
    .orderBy(F.col("manager_level").desc())
)
