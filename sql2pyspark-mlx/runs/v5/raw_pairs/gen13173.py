from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left")
    .groupBy(F.col("departments.id"), F.col("departments.dept_name"))
    .agg(
        F.count(F.col("employees.id")).alias("employee_count"),
        F.round(((100 * F.sum(F.when(F.col("employees.job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
        (F.max(F.col("employees.salary")) - F.min(F.col("employees.salary"))).alias("salary_range"),
    )
    .orderBy("id")
)
