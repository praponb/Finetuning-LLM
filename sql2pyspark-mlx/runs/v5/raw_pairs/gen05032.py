from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("email").like("%@mail.co.th"))
    .groupBy("dept_id")
    .agg(F.avg("salary").alias("avg_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(F.col("d.dept_name"), F.col("d.location"), F.col("t.avg_salary"))
    .orderBy("location", "dept_name")
)
