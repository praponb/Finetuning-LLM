from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.location").isin(["Chiang Mai", "Singapore"]))
    .select(F.col("d.id"), F.col("d.budget"), F.col("d.dept_name"))
    .orderBy(F.col("id").desc())
    .limit(5)
)
