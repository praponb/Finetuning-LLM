from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(~F.col("emp.level").isin(["L3", "L1", "L2"]))
    .select(
        F.col("emp.level").alias("employee_level"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("emp.email").alias("employee_email"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
    .orderBy("employee_level")
)
