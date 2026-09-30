from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("dept_name") == "Engineering").alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .filter(F.col("e.dept_id").isin([1, 8, 12]))
    .select(F.col("e.id"), F.col("e.email"), F.col("e.job_title"))
    .orderBy("email")
)
