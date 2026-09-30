from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("mgr.salary") <= 69000)
    .select(
        F.col("emp.email").alias("employee_email"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.initcap(F.trim(F.col("mgr.full_name"))).alias("full_name_title"),
    )
    .orderBy(F.col("full_name_title").desc())
)
