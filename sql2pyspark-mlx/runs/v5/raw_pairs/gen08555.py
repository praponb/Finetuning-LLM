from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Berlin") & F.col("dept_name").isin(["Engineering", "Sales", "Legal"]) & F.col("location").isin(["London", "Berlin", "Chiang Mai"]))
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3302000, "high").when(F.col("budget") >= 870000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("budget").alias("budget_count"),
    )
    .filter(F.col("total_budget") > 8145000)
    .orderBy("budget_bucket", "dept_name")
    .limit(10)
)
