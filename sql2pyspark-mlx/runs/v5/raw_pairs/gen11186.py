from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("manager_id") == 21)
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .filter(F.col("dt.analyst_count") >= 5)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("dt.analyst_count"),
        F.col("dt.min_salary"),
    )
)
