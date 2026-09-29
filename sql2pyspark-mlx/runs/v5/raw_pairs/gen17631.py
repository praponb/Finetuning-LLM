from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3844000, "high").when(F.col("budget") >= 944000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(F.sum("budget").alias("total_budget"))
    .filter(F.col("total_budget") >= 13500000)
    .select("budget_bucket", "dept_name", "total_budget")
    .orderBy("dept_name")
)
