from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("mgr.email").like("%@startup.io") | F.col("mgr.full_name").like("Ivy%"))
    .select(
        F.col("emp.id"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.level").alias("employee_level"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.coalesce(F.col("mgr.level"), F.lit("N/A")).alias("level_filled"),
    )
)
