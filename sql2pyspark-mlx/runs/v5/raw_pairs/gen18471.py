from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull() | (F.col("hire_date") >= "2024-12-01"))
    .select(
        "id",
        "hire_date",
        "level",
        F.upper("job_title").alias("job_title_upper"),
        F.substring("level", 1, 3).alias("level_prefix"),
    )
)
