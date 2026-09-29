from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("full_name").like("%Kim%"))
    .groupBy("dept_id")
    .agg(
        F.min("salary").alias("min_salary"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0))) / F.count("*")), 1).alias("pct_manager"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "inner")
    .filter(F.col("department_stats.pct_manager") >= 71)
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("department_stats.min_salary"),
        F.col("department_stats.pct_manager"),
    )
)
