from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(~F.col("m.job_title").isin(["Manager", "Intern", "Engineer"]))
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.email").alias("employee_email"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
    .orderBy(F.col("manager_dept_id").desc(), F.col("employee_email").desc())
)
