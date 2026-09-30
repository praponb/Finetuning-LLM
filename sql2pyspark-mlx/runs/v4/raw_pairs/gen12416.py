from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("manager_id").isin([10, 19, 23]))
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary")).otherwise(0)).alias("analyst_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("ds.analyst_salary"))
    .orderBy(F.col("dept_name").desc())
)
