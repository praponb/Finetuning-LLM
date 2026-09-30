from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter((F.col("d.dept_name") == "Legal") & F.col("d.dept_name").isin(["Engineering", "Sales", "HR"]))
    .select(F.col("e.id"), F.col("e.email"), F.col("d.location"))
)
