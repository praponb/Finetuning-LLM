from pyspark.sql import functions as F

result = (
    df_employees
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .groupingSets(
        [["dept_name", "job_title"], ["dept_name"]],
        F.col("p.dept_name").alias("dept_name"),
        F.col("a.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.salary")).alias("min_salary"),
    )
)
