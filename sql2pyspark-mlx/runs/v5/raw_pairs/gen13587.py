from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.is_remote") & F.col("e.manager_id").isin([15, 21, 34]))
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.job_title").alias("manager_job_title"),
    )
    .orderBy("manager_hire_date", F.col("employee_manager_id").desc())
)
