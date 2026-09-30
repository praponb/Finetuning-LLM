from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Designer", "Director"]))
    .select(
        "full_name",
        "is_remote",
        F.coalesce("email", F.lit("Unknown")).alias("email_filled"),
    )
)
