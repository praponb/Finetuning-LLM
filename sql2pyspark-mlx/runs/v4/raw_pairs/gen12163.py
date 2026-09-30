from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.is_remote"))
    .groupBy(F.col("d.id"), F.col("d.location"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
        F.count(F.when(F.col("e.job_title") == "Analyst", 1)).alias("analyst_count"),
    )
    .orderBy("id")
)
