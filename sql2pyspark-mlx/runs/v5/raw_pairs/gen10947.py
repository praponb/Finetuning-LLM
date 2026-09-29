from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "outer")
    .select(
        F.col("e.hire_date"),
        F.col("e.dept_id"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.round(F.col("e.salary"), 0).alias("salary_rounded"),
    )
)
