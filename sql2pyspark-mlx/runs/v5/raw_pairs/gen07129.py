from pyspark.sql import functions as F

result = (
    df_employees
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "inner")
    .groupingSets(
        [["employee_level", "employee_job_title"], ["employee_level"]],
        F.col("e.level").alias("employee_level"),
        F.col("src.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("src.salary")).alias("min_salary"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("employee_job_title").asc_nulls_first())
)
