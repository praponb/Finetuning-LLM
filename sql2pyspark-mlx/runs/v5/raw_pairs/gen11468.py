from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("location").isNotNull()).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .filter(F.col("e.is_remote"))
    .select(F.col("e.id"), F.col("e.salary"), F.col("e.manager_id"))
)
