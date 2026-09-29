from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.full_name").isNotNull())
    .select(
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.level").alias("employee_level"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
    .orderBy("employee_job_title")
)
