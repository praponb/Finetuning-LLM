from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(~(F.month(F.col("m.hire_date")).isin([6, 7, 8]) & (F.col("e.job_title") == "Designer")))
    .select(
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
)
