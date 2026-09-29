from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4124000, "high").when(F.col("budget") >= 1845000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0)).alias("legal_count"),
        F.sum(F.when(F.col("location") == "Singapore", 1).otherwise(0)).alias("singapore_count"),
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
    )
    .orderBy("budget_bucket", F.col("legal_count").desc())
)
