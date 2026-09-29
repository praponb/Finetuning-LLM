from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .orderBy("stddev_salary")
)
