from pyspark.sql import functions as F

base = (
    df_departments
    .filter((F.col("budget") > 3984000) & ((F.col("budget") <= 2766000) | (F.col("budget") >= 2924000)))
)

agg = (
    base
    .groupBy(
        F.when(F.col("budget") >= 4360000, "high").when(F.col("budget") >= 728000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.min("budget").alias("min_budget"),
        F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0)).alias("finance_count"),
    )
)

result = (
    agg
    .select("budget_bucket", "min_budget", "finance_count")
    .orderBy("budget_bucket")
)
