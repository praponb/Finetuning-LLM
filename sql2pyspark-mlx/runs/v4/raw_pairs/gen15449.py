from pyspark.sql import functions as F

result = (
    df_employees
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.budget"))
)
