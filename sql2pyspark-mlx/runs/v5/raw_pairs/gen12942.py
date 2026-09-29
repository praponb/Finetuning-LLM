from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter((F.col("e.job_title") == "Analyst") | F.col("m.is_remote"))
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
)
