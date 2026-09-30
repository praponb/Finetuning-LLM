from pyspark.sql import functions as F

src = df_employees.filter(F.col("full_name").isNotNull())

employees_stats = (
    src
    .groupBy(
        F.when(F.col("salary") >= 152000, "high").when(F.col("salary") >= 49000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.min("salary").alias("min_salary"))
)

result = employees_stats.select("salary_bucket", "min_salary")
