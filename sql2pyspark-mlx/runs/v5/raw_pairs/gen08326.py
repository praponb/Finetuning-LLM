from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.is_remote") | F.upper(F.col("mgr.email")).like("%ACK%"))
    .select(
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
    )
    .orderBy(F.col("manager_hire_date").desc(), "manager_full_name")
)
