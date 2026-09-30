from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.id"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.max(F.col("e.salary")).alias("max_salary"),
        F.sum(F.when(F.col("e.job_title") == "Engineer", 1).otherwise(0)).alias("engineer_count"),
    )
)
