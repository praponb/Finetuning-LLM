from pyspark.sql import functions as F

src = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Legal", "Marketing"]) & (F.col("location").isNotNull() | F.col("dept_name").isNotNull()))
)

departments_stats = (
    src
    .groupBy(
        F.when(F.col("budget") >= 3314000, "high").when(F.col("budget") >= 1034000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.sum("budget").alias("total_budget"),
    )
)

result = (
    departments_stats
    .filter(F.col("total_budget") >= 24033000)
    .select("budget_bucket", "total_budget")
    .orderBy("total_budget")
)
