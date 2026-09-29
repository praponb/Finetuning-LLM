from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter((F.col("mgr.manager_id") == 26) & ~F.col("emp.is_remote"))
    .select(
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.salary").alias("manager_salary"),
        F.concat_ws(",", F.col("mgr.full_name"), F.col("mgr.job_title")).alias("full_name_job_title_key"),
    )
)
