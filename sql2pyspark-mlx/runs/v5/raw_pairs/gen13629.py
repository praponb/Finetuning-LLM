from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([6, 11, 12, 19]) & (F.col("dept_id") == 35))
    .select(
        "id",
        "is_remote",
        "hire_date",
        F.concat_ws(",", "email", "job_title").alias("email_job_title_key"),
    )
    .orderBy(F.col("hire_date").desc(), "id")
)
