from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2193000, 2448000))
    .groupBy(
        F.when(F.col("budget") >= 3794000, "high").when(F.col("budget") >= 1172000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("total_budget") < 12150000)
    .drop("total_budget")
    .orderBy("budget_bucket")
)
