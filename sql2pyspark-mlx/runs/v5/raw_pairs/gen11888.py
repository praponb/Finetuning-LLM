from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 130000, "high").when(F.col("salary") >= 44000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.max("salary").alias("max_salary"),
    )
)
