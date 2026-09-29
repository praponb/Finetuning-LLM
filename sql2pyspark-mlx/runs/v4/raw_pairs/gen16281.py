from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~(F.col("full_name").like("%Olivia%") & F.col("email").like("%@gmail.com")))
    .groupBy("job_title")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
    .filter(F.col("stddev_salary") > 40)
    .orderBy("job_title", F.col("stddev_salary").desc())
)
