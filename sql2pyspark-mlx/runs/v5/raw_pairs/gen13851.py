from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .filter(F.col("e.dept_id").isin([26, 33, 39]))
    .select(F.col("e.id"), F.col("e.email"), F.col("d.budget"))
    .orderBy(F.col("id").desc(), "email")
)
