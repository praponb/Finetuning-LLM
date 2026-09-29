from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.min("salary").alias("min_salary"),
        F.max("hire_date").alias("latest_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("D.min_salary"),
        F.col("D.latest_hire_date"),
    )
)
