from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter((F.col("mgr.job_title") != "Analyst") & F.col("emp.is_remote"))
    .select(
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.level").alias("manager_level"),
    )
    .orderBy(F.col("manager_level").desc())
)
