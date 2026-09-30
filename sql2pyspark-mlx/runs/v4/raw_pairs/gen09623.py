from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3212000, "high").when(F.col("budget") >= 1305000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"))
    .orderBy(F.col("legal_count").desc())
)
