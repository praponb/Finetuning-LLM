from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.salary") < 85000)
    .select(
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
    )
    .orderBy("employee_full_name")
)
