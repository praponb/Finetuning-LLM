from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.year(F.col("e.hire_date")) >= 2024) | (F.col("m.job_title") != "Engineer"))
    .select(
        F.col("e.id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
    .orderBy("id")
    .limit(5)
)
