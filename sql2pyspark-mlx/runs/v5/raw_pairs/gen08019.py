from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        "job_title",
        F.when(F.col("salary") >= 129000, "high").when(F.col("salary") >= 74000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.min("salary").alias("min_salary"),
        F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0)).alias("director_count"),
    )
    .filter(F.col("director_count") >= 23)
    .select("job_title", "salary_bucket", "min_salary", "director_count")
    .orderBy("director_count")
)
