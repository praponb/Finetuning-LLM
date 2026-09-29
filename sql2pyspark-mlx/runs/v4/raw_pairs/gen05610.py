from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(F.col("d.location"), F.col("d.dept_name"), F.col("dt.avg_salary"))
    .orderBy("dept_name")
)
