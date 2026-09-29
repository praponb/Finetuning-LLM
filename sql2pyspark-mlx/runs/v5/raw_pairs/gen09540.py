from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "dept_id")
    .agg(F.countDistinct("salary").alias("unique_job_titles"))
    .filter(F.col("unique_job_titles") >= 6)
    .select("level", "dept_id", "unique_job_titles")
)
