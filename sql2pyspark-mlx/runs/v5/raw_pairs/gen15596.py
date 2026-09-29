from pyspark.sql import functions as F

result = (
    df_employees
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "inner")
    .rollup(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.salary")).alias("avg_salary"),
    )
)
