from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter(F.col("dept_id").isin([4, 11, 18, 19]) & (F.col("hire_date") > "2023-11-19"))
)

is_remote_summary = (
    recent_employees
    .groupBy("is_remote")
    .agg(F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"))
)

result = (
    is_remote_summary
    .select("is_remote", "intern_count")
    .orderBy(F.col("intern_count").desc())
)
