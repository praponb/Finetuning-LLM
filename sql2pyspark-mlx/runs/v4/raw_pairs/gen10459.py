from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.is_remote") | F.month(F.col("m.hire_date")).isin([3, 6, 10]))
    .select(
        F.col("e.salary").alias("employee_salary"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
    .orderBy("employee_manager_id")
)
