from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.min("salary").alias("min_salary"),
        F.avg("salary").alias("avg_salary"),
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
    )
    .orderBy("min_salary")
)
