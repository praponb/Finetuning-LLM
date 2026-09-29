from pyspark.sql import functions as F

result = (
    df_employees
    .alias("s")
    .join(df_employees.alias("dim"), F.col("s.manager_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.job_title").alias("employee_job_title"),
        F.col("dim.level").alias("employee_level"),
        F.col("s.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.salary")), 2).alias("total_salary"),
        F.grouping_id().alias("grouping_level"),
    )
)
