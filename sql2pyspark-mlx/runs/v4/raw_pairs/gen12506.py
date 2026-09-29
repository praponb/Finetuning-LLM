from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter((F.col("e.level") == "L5") & F.col("e.hire_date").between("2023-06-06", "2025-10-01"))
    .select(
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.email").alias("employee_email"),
        F.col("m.salary").alias("manager_salary"),
    )
)
