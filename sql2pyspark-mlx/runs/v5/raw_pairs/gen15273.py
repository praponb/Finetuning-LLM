from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget") < 2006000)
    .select(F.col("d.dept_name"), F.col("e.manager_id"))
    .orderBy("dept_name")
)
