from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.dept_id").isin([14, 19, 31]) & F.col("e.full_name").like("%Dav%"))
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("e.job_title"),
        F.col("e.full_name"),
        F.col("e.salary"),
    )
)
