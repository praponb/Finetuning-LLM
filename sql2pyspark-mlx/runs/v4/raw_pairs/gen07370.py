from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([1, 12, 26]) & F.col("manager_id").isNotNull() & F.col("email").like("%@yahoo.com"))
    .groupBy(
        F.when(F.col("salary") >= 137000, "high").when(F.col("salary") >= 77000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.countDistinct("salary").alias("unique_levels"))
)
