from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("d.dept_name").isin(["Finance", "Marketing", "Sales"]) & (F.col("e.salary") < 94000))
    .select(
        F.col("d.dept_name"),
        F.col("e.hire_date"),
        F.col("e.job_title"),
        F.col("e.dept_id"),
    )
    .orderBy(F.col("dept_id").desc())
)
