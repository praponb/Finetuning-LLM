from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("dept_name").isin(["HR", "Legal"]), "tier_1").otherwise("other").alias("dept_name_group"),
        F.when(F.col("budget") >= 4279000, "high").when(F.col("budget") >= 1723000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
    .filter(F.col("avg_budget") >= 32)
)
