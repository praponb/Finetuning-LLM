from pyspark.sql import functions as F

result = (
    df_employees
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.dept_name") == "Legal")
    .select("src.*", F.col("d.budget"), F.col("d.location"), F.col("d.dept_name"))
)
