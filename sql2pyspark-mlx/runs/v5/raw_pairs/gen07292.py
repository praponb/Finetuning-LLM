from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3990000, "high").when(F.col("budget") >= 846000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("dept_name").isin(["Sales", "Legal"]), "group_a").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_legal"),
        F.sum("budget").alias("total_budget"),
        F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0)).alias("finance_count"),
    )
    .filter(F.col("finance_count") < 49)
    .drop("finance_count")
    .orderBy("dept_name_group")
)
