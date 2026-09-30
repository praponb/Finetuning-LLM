from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-07-16").cast("date"))
    .select(
        "*",
        F.col("salary").cast("int").alias("salary_int"),
        F.upper("full_name").alias("full_name_upper"),
    )
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
        F.when(F.col("salary_int") >= 78, "PREMIUM").otherwise("STANDARD").alias("salary_int_tier"),
    )
    .agg(
        F.avg("salary_int").alias("avg_salary_int"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
        F.count_if(F.col("job_title") == "Director").alias("director_events"),
    )
    .filter(F.col("director_events") >= 1)
    .orderBy(F.col("salary_int_tier").desc_nulls_first())
)
