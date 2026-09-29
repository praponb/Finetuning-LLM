from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-10-19").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
    )
)

result = (
    base
    .groupBy("dept_id", "job_title")
    .agg(
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_rows"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.percentile_approx("days_since_hire_date", 0.9).alias("days_since_hire_date_p90"),
    )
)
