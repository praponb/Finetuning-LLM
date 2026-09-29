from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.email").like("%@yahoo.com"))
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.email").alias("manager_email"),
        F.col("m.level").alias("manager_level"),
        F.col("m.is_remote").cast("int").alias("is_remote_flag"),
    )
    .orderBy("employee_hire_date", "manager_level")
)
