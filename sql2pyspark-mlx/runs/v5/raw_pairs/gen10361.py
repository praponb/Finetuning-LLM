from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNotNull() | (F.col("job_title") == "Designer"))
    .groupBy("level")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.min("salary").alias("min_salary"),
    )
    .orderBy("avg_salary")
)
