from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("level").isin(["L5", "L2", "L3"])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.dept_name").isin(["Finance", "HR", "Engineering"]))
    .select(F.col("d.id"), F.col("d.budget"), F.col("d.location"))
)
