from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("mgr.job_title") != "Engineer")
    .select(
        F.col("emp.id"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.salary").alias("manager_salary"),
    )
)
