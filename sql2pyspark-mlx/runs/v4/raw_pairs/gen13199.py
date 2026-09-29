from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select(
        "id",
        "hire_date",
        "salary",
        "dept_id",
        F.concat_ws(",", "job_title", "email").alias("job_title_email_key"),
        F.date_trunc("YEAR", "hire_date").alias("hire_date_period"),
    )
    .orderBy("hire_date_period")
)
