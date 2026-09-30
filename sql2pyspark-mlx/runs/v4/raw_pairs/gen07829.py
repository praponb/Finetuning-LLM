from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.manager_id").isNotNull() & ~F.col("m.is_remote"))
    .select(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.email").alias("manager_email"),
        F.col("m.salary").alias("manager_salary"),
    )
    .orderBy(F.col("manager_salary").desc())
)
