from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.dept_id").isin([4, 8, 10, 15]) & F.col("m.is_remote"))
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.level").alias("manager_level"),
        F.col("m.salary").alias("manager_salary"),
    )
    .orderBy(F.col("manager_salary").desc(), "id")
    .limit(1)
)
