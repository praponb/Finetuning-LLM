from pyspark.sql import functions as F

rollup_totals = (
    df_employees
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.dept_name").alias("dept_name"),
        F.col("s.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("s.salary")).alias("total_salary"),
    )
)

result = (
    rollup_totals
    .select("dept_name", "job_title", "rows_total")
    .orderBy(F.col("dept_name").asc_nulls_last())
)
