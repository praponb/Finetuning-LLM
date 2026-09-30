from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4327000, "high").when(F.col("budget") >= 1027000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_legal"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("pct_legal") >= 29)
    .select("budget_bucket", "pct_legal", "total_budget")
    .orderBy(F.col("total_budget").desc(), F.col("budget_bucket").desc())
)
