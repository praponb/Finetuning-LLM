from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter((F.col("mgr.level") != "L4") | (F.col("emp.level") == "L3"))
    .select(
        F.col("emp.id"),
        F.col("emp.level").alias("employee_level"),
        F.col("emp.email").alias("employee_email"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.col("mgr.email").alias("manager_email"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.salary").cast("int").alias("salary_int"),
    )
    .orderBy("manager_manager_id", "employee_level")
)
