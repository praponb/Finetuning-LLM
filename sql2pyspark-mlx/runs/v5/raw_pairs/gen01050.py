from pyspark.sql import functions as F

rollup_totals = (
    df_employees
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["employee_job_title", "employee_level"], ["employee_job_title"], []],
        F.col("d.job_title").alias("employee_job_title"),
        F.col("a.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.salary")).alias("avg_salary"),
    )
)

result = (
    rollup_totals
    .select("employee_level", "rows_total")
    .orderBy(F.col("employee_level").desc_nulls_last())
)
