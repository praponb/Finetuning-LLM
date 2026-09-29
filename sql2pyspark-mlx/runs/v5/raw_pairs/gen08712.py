from pyspark.sql import functions as F

result = (
    df_employees
    .alias("s")
    .join(df_employees.alias("dim"), F.col("s.manager_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["employee_level", "employee_job_title", "employee_level"], ["employee_level", "employee_job_title"], ["employee_level"], []],
        F.col("dim.level").alias("employee_level"),
        F.col("dim.job_title").alias("employee_job_title"),
        F.col("s.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("s.salary")).alias("avg_salary"),
        F.grouping_id().alias("grouping_level"),
    )
)
