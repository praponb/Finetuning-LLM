from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.dept_id").isin([34, 37, 38]) & F.upper(F.col("e.email")).like("%LIC%"))
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("e.job_title"),
        F.col("e.salary"),
        F.col("e.dept_id"),
    )
    .orderBy("location", F.col("budget").desc())
)
