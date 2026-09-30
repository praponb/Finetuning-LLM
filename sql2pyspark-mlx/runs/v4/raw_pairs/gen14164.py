from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-10-16").cast("date")) & F.col("job_title").isNotNull())
    .withColumn("salary_adjusted", F.round((F.col("salary") * 0.85), 2))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("UNKNOWN")))
)

result = (
    base
    .select("job_title", "id")
    .subtract(
        df_employees
        .filter(F.col("hire_date") <= "2023-08-13")
        .select("job_title", "id")
    )
)
