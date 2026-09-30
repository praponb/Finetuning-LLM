from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("level").isin(["L2", "L4"]))
    .select(
        "*",
        F.last_day("hire_date").alias("hire_date_month_end"),
        F.trim("email").alias("clean_email"),
    )
)

result = (
    filtered
    .select("job_title", "id")
    .intersectAll(
        df_employees
        .filter(F.col("level").isin(["L3", "L2", "L4"]))
        .select("job_title", "id")
    )
)
