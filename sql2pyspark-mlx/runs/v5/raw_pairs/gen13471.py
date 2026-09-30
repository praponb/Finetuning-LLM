from pyspark.sql import functions as F

with_dims = (
    df_employees
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "left")
    .filter(F.col("e.hire_date").between("2023-08-01", "2023-12-01"))
    .select(
        "src.*",
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.level").alias("employee_level"),
        F.col("e.full_name").alias("employee_full_name"),
    )
)

result = (
    with_dims
    .groupBy(F.date_trunc("QUARTER", "hire_date").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.avg("salary").alias("avg_salary"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("employee_job_title") == "Engineer", 1)), F.lit(0)), 2).alias("salary_per_engineer"),
    )
    .filter(F.col("avg_salary") > 0)
    .orderBy(F.col("avg_salary").desc())
)
