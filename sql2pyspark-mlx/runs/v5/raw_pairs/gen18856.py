from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([12, 14, 38]))
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0)).alias("manager_count"),
    )
    .orderBy(F.col("manager_count").desc())
)
