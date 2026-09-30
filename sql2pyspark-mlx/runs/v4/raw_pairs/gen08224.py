from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.dept_name").isin(["Legal", "Sales"]))
    .select(
        F.col("e.id"),
        F.col("e.job_title"),
        F.col("e.hire_date"),
        F.col("e.full_name"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("d.budget"),
    )
    .orderBy(F.col("id").desc())
    .limit(50)
)
