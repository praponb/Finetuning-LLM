from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("employee_count"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0))) / F.count("*")), 1).alias("pct_analyst"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.pct_analyst") >= 60)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("ds.employee_count"),
        F.col("ds.pct_analyst"),
    )
)
