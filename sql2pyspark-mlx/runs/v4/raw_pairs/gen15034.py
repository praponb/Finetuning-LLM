from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("level").isin(["L2", "L4"]))
    .select(
        "id",
        "email",
        F.concat_ws("-", "email", "job_title").alias("email_job_title_key"),
        F.last_day("hire_date").alias("hire_date_month_end"),
    )
)
