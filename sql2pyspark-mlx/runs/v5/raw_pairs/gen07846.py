from pyspark.sql import functions as F

result = (
    df_employees
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "inner")
    .cube(
        F.col("e.level").alias("employee_level"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("a.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("a.salary")).alias("total_salary"),
        F.grouping_id().alias("gid"),
    )
)
