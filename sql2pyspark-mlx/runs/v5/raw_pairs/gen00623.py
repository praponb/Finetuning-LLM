from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 30)
    .groupBy(
        F.when(F.col("salary") >= 119000, "high").when(F.col("salary") >= 84000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
        F.countDistinct("salary").alias("unique_job_titles"),
    )
    .filter(F.col("l4_salary") > 71)
)
