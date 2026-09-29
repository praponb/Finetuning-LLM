from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("m.hire_date") <= "2024-10-01") | (F.col("m.salary") <= 144000))
    .select(
        F.col("e.salary").alias("employee_salary"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.full_name").alias("manager_full_name"),
    )
)
