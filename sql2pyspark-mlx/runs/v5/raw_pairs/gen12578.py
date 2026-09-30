from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 111000)
    .groupBy(
        F.when(F.col("salary") >= 150000, "high").when(F.col("salary") >= 57000, "mid").otherwise("low").alias("salary_bucket"),
        "is_remote",
    )
    .agg(F.countDistinct("salary").alias("unique_levels"))
    .orderBy("salary_bucket", F.col("unique_levels").desc(), "is_remote")
    .limit(10)
)
