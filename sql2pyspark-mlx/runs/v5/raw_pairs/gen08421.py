from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.id"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.email").alias("manager_email"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
