from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.dept_id").isin([17, 21, 25, 31]) & (F.col("e.manager_id") == 17))
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.email").alias("employee_email"),
        F.col("m.level").alias("manager_level"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.salary").alias("manager_salary"),
    )
    .orderBy("employee_email", "id")
    .limit(20)
)
