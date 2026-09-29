from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.job_title").isin(["Director", "Analyst", "Engineer"]))
    .select(
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.level").alias("employee_level"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.level").alias("manager_level"),
    )
    .orderBy("manager_hire_date")
)
