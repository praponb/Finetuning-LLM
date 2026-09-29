from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("email").like("%@GMAIL.COM"))
    .groupBy("dept_id")
    .agg(F.min("salary").alias("min_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(F.col("d.dept_name"), F.col("d.location"), F.col("dt.min_salary"))
)
