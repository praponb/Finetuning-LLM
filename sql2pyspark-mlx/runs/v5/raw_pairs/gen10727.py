from pyspark.sql import functions as F

result = (
    df_employees
    .alias("s")
    .join(df_employees.alias("dim"), F.col("s.manager_id") == F.col("dim.id"), "inner")
    .rollup(
        F.col("dim.job_title").alias("employee_job_title"),
        F.col("dim.level").alias("employee_level"),
        F.col("s.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("s.salary")).alias("total_salary"),
    )
    .orderBy(F.col("employee_level").asc_nulls_first())
)
