from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3041000, "high").when(F.col("budget") >= 1936000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.countDistinct("budget").alias("unique_locations"))
    .filter(F.col("unique_locations") < 5)
    .orderBy(F.col("budget_bucket").desc())
)
