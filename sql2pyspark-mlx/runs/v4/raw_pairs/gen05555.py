from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2024-01-22").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.upper(F.trim("email")).alias("email_normalized"),
    )
)

result = (
    filtered
    .rollup(
        F.when(F.col("salary") >= 139000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Intern", F.col("salary"))).alias("intern_total_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum(F.when(F.col("job_title") == "Director", F.col("salary")).otherwise(0)).alias("director_salary"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_rows"),
        F.count_if(F.col("job_title") == "Engineer").alias("engineer_events"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
    )
    .filter(F.col("engineer_events") >= 1)
)
