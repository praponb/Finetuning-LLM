from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        "job_title",
        F.when(F.col("salary") >= 127000, "high").when(F.col("salary") >= 63000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
        F.min("salary").alias("min_salary"),
    )
)
