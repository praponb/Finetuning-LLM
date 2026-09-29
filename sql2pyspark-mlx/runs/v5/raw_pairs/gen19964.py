from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0)).alias("designer_count"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "inner")
    .filter(F.col("department_stats.designer_count") > 71)
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("departments.location"),
        F.col("department_stats.designer_count"),
    )
)
