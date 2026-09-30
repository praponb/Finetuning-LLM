from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.upper(F.trim(F.col("e.email"))) == "IVY@GMAIL.COM")
    .select(
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.level").alias("manager_level"),
        F.col("m.email").alias("manager_email"),
    )
    .orderBy("manager_level")
)
