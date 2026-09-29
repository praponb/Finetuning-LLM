from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 95000)
    .select(
        "id",
        "hire_date",
        "salary",
        "is_remote",
        F.concat("email", F.lit(", "), "job_title").alias("email_job_title"),
    )
)
