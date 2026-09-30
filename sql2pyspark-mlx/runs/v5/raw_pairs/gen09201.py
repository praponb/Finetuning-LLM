from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.level").isin(["L1", "L3"]))
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
)
