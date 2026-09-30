from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "level",
        "manager_id",
        F.col("salary").cast("int").alias("salary_int"),
        F.concat_ws(",", "email", "job_title").alias("email_job_title_key"),
    )
)
