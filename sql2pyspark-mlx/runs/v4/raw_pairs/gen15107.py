from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.email").like("%@startup.io") | (F.upper(F.trim(F.col("m.email"))) == "DAVID.LEE@GMAIL.COM"))
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.job_title").alias("manager_job_title"),
    )
    .orderBy(F.col("manager_dept_id").desc(), F.col("employee_salary").desc())
)
