from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.dept_id").isin([1, 2, 20, 24]))
    .select(
        F.col("emp.id"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.salary").alias("manager_salary"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
