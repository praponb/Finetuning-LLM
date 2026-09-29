from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("dept_id") == 16)
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0))) / F.count("*")), 1).alias("pct_manager"),
        F.count("*").alias("employee_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.pct_manager") > 24)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("ds.pct_manager"),
        F.col("ds.employee_count"),
    )
    .orderBy("budget")
)
