from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.salary").between(64000, 98000) & (F.lower(F.trim(F.col("e.email"))) == "jack@startup.io"))
    .select(
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
    .orderBy(F.col("employee_dept_id").desc(), "manager_manager_id")
)
