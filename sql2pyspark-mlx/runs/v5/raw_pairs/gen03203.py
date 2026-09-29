from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.countDistinct("salary").alias("unique_job_titles"),
        F.min("salary").alias("min_salary"),
        F.countDistinct("salary").alias("unique_managers"),
    )
    .orderBy("unique_job_titles")
)
