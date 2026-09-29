from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "full_name",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.date_sub("hire_date", 63).alias("hire_date_minus_63d"),
    )
)
