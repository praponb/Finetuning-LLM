from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@outlook.com") & F.col("dept_id").isin([3, 7]))
    .select(
        "id",
        "job_title",
        "level",
        "email",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
    )
)
