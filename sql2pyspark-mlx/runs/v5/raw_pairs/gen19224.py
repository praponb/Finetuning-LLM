from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(3083000, 3605000))
    .groupBy(
        F.when(F.col("budget") >= 3273000, "high").when(F.col("budget") >= 1613000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0)).alias("marketing_count"),
    )
    .orderBy(F.col("budget_bucket").desc())
)
