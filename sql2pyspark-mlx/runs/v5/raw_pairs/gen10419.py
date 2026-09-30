from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.col("full_name").like("FR%"))
    .withColumn("days_since_hire_date", F.datediff(F.current_date(), "hire_date"))
    .withColumn("hire_date_plus_months", F.add_months("hire_date", 6))
    .withColumn("level_filled", F.coalesce("level", F.lit("OTHER")))
)

result = (
    cleaned
    .select("job_title")
    .intersectAll(
        df_employees
        .filter(F.col("email").like("%@mail.co.th"))
        .select("job_title")
    )
)
