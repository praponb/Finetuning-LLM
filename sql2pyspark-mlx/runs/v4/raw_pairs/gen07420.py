from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Legal", "HR"]) & F.col("budget").between(2447000, 3897000))
    .groupBy(
        F.when(F.col("dept_name").isin(["Engineering", "Marketing"]), "tier_1").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0))) / F.count("*")), 1).alias("pct_finance"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Sales", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sales"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("dept_name_group").desc())
)
