from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.hire_date").between("2023-04-22", "2024-12-01") | F.col("e.manager_id").isNotNull())
    .select(
        F.col("e.level").alias("employee_level"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.email").alias("manager_email"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
)
