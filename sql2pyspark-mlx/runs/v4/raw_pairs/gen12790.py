from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(F.col("d.dept_name"), F.col("e.email"), F.col("e.job_title"))
    .orderBy("job_title")
)
