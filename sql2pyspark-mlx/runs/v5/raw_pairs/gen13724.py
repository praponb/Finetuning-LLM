from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 164000, "high").when(F.col("salary") >= 42000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.sum("salary").alias("total_salary"))
    .filter(F.col("total_salary") >= 550000)
)
