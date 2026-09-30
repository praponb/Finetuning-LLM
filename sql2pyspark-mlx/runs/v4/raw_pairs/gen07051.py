from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.email").like("%@outlook.com"))
    .select(
        F.col("e.id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.level").alias("manager_level"),
        F.col("e.salary").cast("int").alias("salary_int"),
    )
    .orderBy("employee_dept_id", "id")
    .limit(5)
)
