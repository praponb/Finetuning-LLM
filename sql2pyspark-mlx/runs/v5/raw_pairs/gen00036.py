from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter((F.year(F.col("m.hire_date")) >= 2023) & F.col("e.email").like("%@startup.io"))
    .select(
        F.col("e.id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.email").alias("manager_email"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.last_day(F.col("e.hire_date")).alias("hire_date_month_end"),
    )
)
