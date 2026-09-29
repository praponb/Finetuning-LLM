from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.is_remote") & F.col("emp.full_name").isNull())
    .select(
        F.col("emp.id"),
        F.col("emp.level").alias("employee_level"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
)
