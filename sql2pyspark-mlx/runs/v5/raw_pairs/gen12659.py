from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter((F.col("mgr.dept_id") == 24) & F.lower(F.col("emp.full_name")).like("%lic%"))
    .select(
        F.col("emp.id"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
    )
    .orderBy(F.col("employee_full_name").desc())
)
