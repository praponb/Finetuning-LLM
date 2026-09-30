from pyspark.sql import functions as F

result = (
    df_employees
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["employee_level", "employee_job_title"], ["employee_level"]],
        F.col("d.level").alias("employee_level"),
        F.col("s.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.salary")).alias("min_salary"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("employee_job_title").desc_nulls_first())
)
