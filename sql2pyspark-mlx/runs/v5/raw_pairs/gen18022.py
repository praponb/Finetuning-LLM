from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"))
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "inner")
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("t.engineer_count"),
    )
    .orderBy("engineer_count", "id")
    .limit(20)
)
