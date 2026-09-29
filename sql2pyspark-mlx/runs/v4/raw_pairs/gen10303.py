from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0)).alias("manager_count"),
    )
    .orderBy(F.col("manager_count").desc())
)
