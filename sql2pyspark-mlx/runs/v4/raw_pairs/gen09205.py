from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(~F.col("mgr.job_title").isin(["Designer", "Director", "Intern"]) & (F.col("mgr.salary") < 84000))
    .select(
        F.col("emp.id"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("mgr.level").alias("manager_level"),
    )
    .orderBy("manager_level")
)
