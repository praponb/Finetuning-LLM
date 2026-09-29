from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3087000, "high").when(F.col("budget") >= 1166000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("budget").alias("budget_count"))
    .filter(F.col("budget_count") >= 7)
    .select("budget_bucket", "budget_count")
    .orderBy("budget_bucket")
    .limit(20)
)
