from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 4476000, "high").when(F.col("budget") >= 1964000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("budget_bucket").desc())
)
