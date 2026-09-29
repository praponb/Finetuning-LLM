from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(~F.col("m.is_remote"))
    .select(
        F.col("e.id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.level").alias("employee_level"),
        F.col("m.email").alias("manager_email"),
        F.col("m.level").alias("manager_level"),
        F.last_day(F.col("e.hire_date")).alias("hire_date_month_end"),
    )
)
