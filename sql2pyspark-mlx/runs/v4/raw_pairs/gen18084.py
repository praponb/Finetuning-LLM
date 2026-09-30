from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "is_remote",
        "job_title",
        "dept_id",
        F.trim("full_name").alias("clean_full_name"),
    )
)
