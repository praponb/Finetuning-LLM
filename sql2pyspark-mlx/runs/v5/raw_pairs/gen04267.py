from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.manager_id").isin([3, 10, 23]) & F.col("m.email").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.email").alias("employee_email"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.salary").alias("manager_salary"),
    )
    .orderBy("employee_email")
)
