from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.month("hire_date").isin([3, 6, 11]))
    .groupBy(
        "job_title",
        F.when(F.col("salary") >= 163000, "high").when(F.col("salary") >= 60000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.avg("salary").alias("avg_salary"))
    .filter(F.col("avg_salary") >= 60000)
    .orderBy("salary_bucket", "avg_salary", "job_title")
    .limit(10)
)
