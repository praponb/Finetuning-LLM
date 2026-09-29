from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.budget").between(1201000, 1403000) & F.col("d.location").isin(["Bangkok", "New York", "Singapore"]))
    .select(
        F.col("e.id"),
        F.col("e.salary"),
        F.col("e.is_remote"),
        F.col("e.job_title"),
        F.col("d.dept_name"),
        F.col("d.budget"),
    )
)
