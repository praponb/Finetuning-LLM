from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_levels"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("t.unique_levels"))
)
