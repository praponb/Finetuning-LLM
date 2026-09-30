from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.round("salary", 0).alias("salary_rounded"),
        F.coalesce("job_title", F.lit("Unknown")).alias("job_title_filled"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("job_title_filled_2"),
    )
)

result = (
    base
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "left")
    .rollup(
        F.col("p.location").alias("location"),
        F.col("src.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("src.salary_rounded")), 2).alias("avg_salary_rounded"),
        F.grouping_id().alias("level_id"),
    )
)
