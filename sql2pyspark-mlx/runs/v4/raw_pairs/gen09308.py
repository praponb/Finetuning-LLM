from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter((F.col("location").isin(["Singapore", "Tokyo"]) & (F.col("budget") >= 1154000)) | ~F.col("location").isin(["Singapore", "Berlin"]))
    .withColumn("budget_whole", F.floor("budget"))
)

summary = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 3055000, "high").when(F.col("budget") >= 1513000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("budget_whole").alias("budget_whole_count"))
)

result = (
    summary
    .orderBy("budget_bucket")
    .limit(50)
)
