from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "level",
        "is_remote",
        "salary",
        F.concat("job_title", F.lit("/"), "full_name").alias("job_title_full_name"),
    )
)
