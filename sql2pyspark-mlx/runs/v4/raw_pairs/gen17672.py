from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") < 124000)
    .select(
        "manager_id",
        "job_title",
        "level",
        F.upper("email").alias("email_upper"),
        F.lower("level").alias("level_lower"),
    )
)
