from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.length(F.col("e.email")) >= 6)
    .select(
        F.col("e.id"),
        F.col("e.email").alias("employee_email"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.job_title").alias("manager_job_title"),
    )
    .orderBy(F.col("id").desc(), F.col("manager_manager_id").desc())
)
