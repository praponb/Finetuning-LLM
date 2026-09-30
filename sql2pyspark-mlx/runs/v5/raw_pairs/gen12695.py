from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("emp.manager_id") == 18)
    .select(
        F.col("emp.salary").alias("employee_salary"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.salary").alias("manager_salary"),
        F.coalesce(F.col("mgr.full_name"), F.lit("Unknown")).alias("full_name_filled"),
    )
)
