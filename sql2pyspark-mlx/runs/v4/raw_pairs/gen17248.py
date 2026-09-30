from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.id"),
        F.col("emp.email").alias("employee_email"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.round(F.col("emp.salary"), 0).alias("salary_rounded"),
    )
    .orderBy("id")
)
