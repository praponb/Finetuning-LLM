from pyspark.sql import functions as F

result = (
    df_employees
    .rollup("dept_id")
    .agg(
        F.count_if(F.col("job_title") == "Director").alias("director_events"),
        F.avg("salary").alias("avg_salary"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
    )
)
