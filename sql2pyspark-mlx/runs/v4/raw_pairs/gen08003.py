from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 153000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.count_if(F.col("job_title") == "Manager").alias("manager_events"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
    )
    .orderBy(F.col("manager_events").desc_nulls_first())
)
