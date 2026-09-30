from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .filter(F.col("dt.engineer_count") >= 8)
    .select(F.col("d.location"), F.col("d.budget"), F.col("dt.engineer_count"))
    .orderBy(F.col("budget").desc())
)
