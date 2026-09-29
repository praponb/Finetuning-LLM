from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("employee_count"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.employee_count") > 8)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("ds.employee_count"),
        F.col("ds.min_salary"),
    )
    .orderBy("budget")
)
