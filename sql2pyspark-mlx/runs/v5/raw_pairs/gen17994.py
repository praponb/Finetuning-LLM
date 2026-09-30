from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.dept_id").isin([20, 24, 32]))
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
)
