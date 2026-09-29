from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 13)
    .groupBy("job_title", "level")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"),
    )
)
