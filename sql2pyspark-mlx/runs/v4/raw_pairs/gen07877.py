from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2913000, 3369000))
    .groupBy(
        F.when(F.col("budget") >= 3987000, "high").when(F.col("budget") >= 605000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget")).otherwise(0)).alias("sales_budget"),
    )
    .filter(F.col("sales_budget") >= 73)
    .select("budget_bucket", "sales_budget")
    .orderBy(F.col("budget_bucket").desc())
)
