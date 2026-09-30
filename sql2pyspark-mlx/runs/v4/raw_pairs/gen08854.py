from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.level").alias("employee_level"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
    .orderBy("id")
)
