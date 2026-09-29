from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.upper(F.trim(F.col("e.email"))) == "CAROL@COMPANY.CO")
    .select(
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.full_name").alias("manager_full_name"),
    )
    .orderBy(F.col("manager_manager_id").desc())
)
