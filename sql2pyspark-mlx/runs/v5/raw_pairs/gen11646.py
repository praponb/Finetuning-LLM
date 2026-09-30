from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.id"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("mgr.email").alias("manager_email"),
    )
    .orderBy(F.col("id").desc())
)
