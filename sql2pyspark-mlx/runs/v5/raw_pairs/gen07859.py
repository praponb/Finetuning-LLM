from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("hire_date") < "2025-01-27")
    .select(
        "*",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("job_title", F.lit("OTHER")).alias("job_title_filled"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["location", "level"], ["location"]],
        F.col("p.location").alias("location"),
        F.col("src.level").alias("level"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.salary")).alias("max_salary"),
    )
)
