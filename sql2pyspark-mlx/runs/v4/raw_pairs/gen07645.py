from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.email").like("%@company.co"))
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.email").alias("employee_email"),
        F.col("m.level").alias("manager_level"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
)
