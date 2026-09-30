from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & (F.col("hire_date") <= "2025-10-01") & ~F.col("is_remote"))
    .groupBy(
        F.when(F.col("salary") >= 152000, "high").when(F.col("salary") >= 65000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.avg("salary").alias("avg_salary"))
    .orderBy(F.col("salary_bucket").desc())
)
