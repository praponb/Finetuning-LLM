from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote") | F.col("full_name").like("bob%"))
    .agg(
        F.countDistinct("salary").alias("unique_job_titles"),
        F.min("salary").alias("min_salary"),
    )
)
