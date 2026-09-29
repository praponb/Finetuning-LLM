from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("dept_id").isin([1, 20, 26, 27]))
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(F.col("d.location"), F.col("d.budget"), F.col("ds.designer_count"))
    .orderBy(F.col("designer_count").desc())
)
