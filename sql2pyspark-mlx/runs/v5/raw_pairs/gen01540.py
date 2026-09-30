from pyspark.sql import functions as F

result = (
    df_employees
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "inner")
    .groupingSets(
        [["employee_level", "employee_job_title"], ["employee_level"]],
        F.col("e.level").alias("employee_level"),
        F.col("e.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("src.salary")), 2).alias("avg_salary"),
    )
)
