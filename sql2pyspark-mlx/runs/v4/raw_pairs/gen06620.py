from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.email").alias("employee_email"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
    .orderBy(F.col("employee_email").desc(), "employee_manager_id", "id")
    .limit(5)
)
