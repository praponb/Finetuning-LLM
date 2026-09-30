from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id", "is_remote")
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0)).alias("designer_count"),
    )
    .filter(F.col("designer_count") >= 54)
    .orderBy(F.col("dept_id").desc(), F.col("designer_count").desc(), "is_remote")
    .limit(5)
)
