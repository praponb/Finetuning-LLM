from pyspark.sql import functions as F

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-05-23").cast("date")) & (F.col("salary") > 145000))
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "is_remote",
        F.regexp_replace("full_name", " ", "").alias("full_name_slug"),
        F.round("salary", 2).alias("salary_rounded"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("resolved_job_title"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .groupingSets(
        [["dept_name", "job_title"], ["dept_name"]],
        F.col("d.dept_name").alias("dept_name"),
        F.col("s.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("s.salary_rounded")).alias("total_salary_rounded"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
