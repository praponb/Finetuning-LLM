from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 123000, "high").when(F.col("salary") >= 51000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)
