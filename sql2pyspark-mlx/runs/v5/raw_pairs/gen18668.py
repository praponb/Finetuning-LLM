from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote") & ~F.col("is_remote"))
    .groupBy("dept_id", "is_remote")
    .agg(
        F.countDistinct("salary").alias("unique_job_titles"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
    .filter(F.col("unique_job_titles") > 4)
    .orderBy("unique_job_titles", "salary_range")
)
