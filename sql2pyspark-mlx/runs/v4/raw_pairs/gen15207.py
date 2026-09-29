from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("location") != "Bangkok").alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .filter(F.col("e.dept_id").isin([7, 24, 26]))
    .select(F.col("e.id"), F.col("e.hire_date"), F.col("e.full_name"))
    .orderBy("hire_date", "full_name")
)
