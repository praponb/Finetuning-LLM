from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.dept_id") == 30)
    .select(
        F.col("e.id"),
        F.col("e.job_title"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("d.location"),
    )
)
