from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.job_title") != "Designer")
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
    .orderBy(F.col("id").desc(), "employee_salary")
    .limit(5)
)
