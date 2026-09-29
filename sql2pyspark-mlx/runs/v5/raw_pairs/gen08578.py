from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(~F.col("emp.level").isin(["L1", "L5", "L4"]))
    .select(
        F.col("emp.id"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("emp.level").alias("employee_level"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.email").alias("manager_email"),
        F.floor(F.col("emp.salary")).alias("salary_whole"),
    )
)
