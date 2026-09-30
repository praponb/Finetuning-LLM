from pyspark.sql import functions as F

result = (
    df_employees
    .agg(
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
        F.min("hire_date").alias("min_hire_date"),
    )
)
