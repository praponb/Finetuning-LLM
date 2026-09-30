from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("job_title").isin(["Designer", "Engineer"]))
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
        F.avg("salary").alias("avg_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(F.col("d.location"), F.col("t.intern_count"), F.col("t.avg_salary"))
    .orderBy(F.col("intern_count").desc(), F.col("location").desc())
)
