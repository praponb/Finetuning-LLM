from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
    .filter(F.col("avg_salary") > 38)
    .orderBy(F.col("job_title").desc())
)
