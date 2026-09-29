from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.id"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
    )
)
