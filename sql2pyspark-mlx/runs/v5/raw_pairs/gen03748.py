from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.job_title").isin(["Analyst", "Engineer"]))
    .select(
        F.col("e.id"),
        F.col("e.manager_id"),
        F.col("e.is_remote"),
        F.col("e.hire_date"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
    )
)
