from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.upper(F.trim("full_name")).alias("full_name_normalized"),
    )
)

cleaned = (
    valid
    .filter((F.col("hire_date") > F.lit("2023-05-16").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.dayofmonth("hire_date").alias("hire_date_day"),
    )
)

base = (
    cleaned
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-03-22"))) & F.col("job_title").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        "hire_date_day",
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_filled"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_filled_2"),
    )
)

result = (
    base
    .rollup(
        F.when(F.col("hire_date_day") >= 67, "HIGH").otherwise("LOW").alias("hire_date_day_tier"),
    )
    .agg(
        F.round(F.sum("hire_date_day") / F.nullif(F.count(F.when(F.col("level") == "L2", 1)), F.lit(0)), 2).alias("hire_date_day_per_l2"),
        F.percentile_approx("hire_date_day", [0.25, 0.5, 0.75]).alias("hire_date_day_quartiles"),
        F.sum(F.when(F.col("level") == "L1", F.col("hire_date_day")).otherwise(0)).alias("l1_hire_date_day"),
    )
    .filter(F.col("hire_date_day_per_l2") >= 0)
    .orderBy(F.col("hire_date_day_tier").asc_nulls_last())
)
