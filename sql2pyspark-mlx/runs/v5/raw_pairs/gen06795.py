from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("d.budget") < 2259000)
    .groupBy(F.col("d.id"), F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.sum(F.when(F.col("e.job_title") == "Analyst", F.col("e.salary")).otherwise(0)).alias("analyst_salary"),
        F.min(F.col("e.salary")).alias("min_salary"),
    )
    .orderBy("dept_name")
)
