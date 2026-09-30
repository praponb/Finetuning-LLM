from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("dept_id").isin([19, 28, 36]))
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_engineer"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("DepartmentTotals"), F.col("departments.id") == F.col("DepartmentTotals.dept_id"), "inner")
    .select(F.col("departments.location"), F.col("DepartmentTotals.pct_engineer"))
    .orderBy("location")
)
