from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.salary") > 92000)
    .select(
        F.col("e.salary").alias("employee_salary"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.lower(F.col("m.full_name")).alias("full_name_lower"),
    )
    .orderBy(F.col("employee_salary").desc(), "full_name_lower")
)
