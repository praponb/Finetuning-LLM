from pyspark.sql import functions as F

src = (
    df_departments
    .filter((F.col("location") != "Berlin") & F.col("budget").isNotNull())
    .withColumn("budget_whole", F.ceil("budget"))
)

budget_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("budget") >= 4470000, "high").when(F.col("budget") >= 679000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.max("budget").alias("max_budget"),
        F.max("budget_whole").alias("max_budget_whole"),
    )
)

result = budget_bucket_summary
