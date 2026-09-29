from pyspark.sql import functions as F

base = df_employees.filter(F.col("salary") <= 84000)

summary = (
    base
    .groupBy("is_remote")
    .agg(F.countDistinct("salary").alias("unique_job_titles"))
)

result = (
    summary
    .filter(F.col("unique_job_titles") < 8)
    .select("is_remote", "unique_job_titles")
)
