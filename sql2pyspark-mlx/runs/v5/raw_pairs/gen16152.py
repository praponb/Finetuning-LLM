from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("full_name").like("bo%")).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .select(F.col("d.id"), F.col("d.dept_name"))
    .orderBy("id", F.col("dept_name").desc())
    .limit(3)
)
