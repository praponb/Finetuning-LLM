from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("mgr.manager_id").isin([3, 7, 32]))
    .select(
        F.col("emp.id"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
    )
)
