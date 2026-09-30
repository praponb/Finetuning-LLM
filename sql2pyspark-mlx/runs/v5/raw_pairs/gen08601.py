from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.salary") < 80000)
    .select(
        F.col("e.level").alias("employee_level"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
)
