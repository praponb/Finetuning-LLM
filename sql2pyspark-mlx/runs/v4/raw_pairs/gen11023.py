from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .filter(F.year(F.col("e.hire_date")) == 2023)
    .select(
        F.col("e.full_name"),
        F.col("e.hire_date"),
        F.col("e.job_title"),
        F.col("d.location"),
    )
    .orderBy(F.col("location").desc())
)
