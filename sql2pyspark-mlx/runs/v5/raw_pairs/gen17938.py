from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.manager_id").isNull() & F.col("e.is_remote"))
    .select(
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.job_title").alias("manager_job_title"),
    )
    .orderBy("manager_job_title")
)
