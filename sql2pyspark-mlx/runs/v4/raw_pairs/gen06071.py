from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("l2_count") < 49)
    .orderBy(F.col("max_salary").desc())
)
