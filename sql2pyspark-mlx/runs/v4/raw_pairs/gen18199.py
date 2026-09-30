from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.manager_id").isNull() & (F.upper(F.trim(F.col("mgr.email"))) == "CAROL@COMPANY.CO"))
    .select(
        F.col("emp.id"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.email").alias("employee_email"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
    )
    .orderBy("manager_hire_date")
)
