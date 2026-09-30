from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.budget").between(2941000, 3181000))
    .select(F.col("d.budget"), F.col("e.is_remote"), F.col("e.level"), F.col("e.email"))
)
