from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("m.manager_id") == 15) | (F.upper(F.trim(F.col("e.full_name"))) == "IVY CHEN"))
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
)
