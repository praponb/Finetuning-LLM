from pyspark.sql import functions as F

result = (
    df_employees
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.hire_date").alias("employee_hire_date"),
        F.col("d.email").alias("employee_email"),
        F.col("d.full_name").alias("employee_full_name"),
    )
)
