from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Engineer", "Designer"]))
    .select(
        "salary",
        F.concat_ws("/", "email", "job_title").alias("email_job_title_key"),
    )
)
